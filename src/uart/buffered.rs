//! Buffered UART driver.
use core::future::Future;
use core::slice;
use core::sync::atomic::{AtomicU32, Ordering};

use embassy_hal_internal::atomic_ring_buffer::RingBuffer;
use embassy_hal_internal::interrupt::InterruptExt;

use super::*;

/// Buffered UART state
pub struct State {
    tx_waker: AtomicWaker,
    tx_buf: RingBuffer,
    rx_waker: AtomicWaker,
    rx_buf: RingBuffer,
    rx_error: AtomicU32,
}

// these must match bits in STAT register.
const RXE_NOISE: u32 = 1 << 15;
const RXE_PARITYERR: u32 = 1 << 14;
const RXE_FRAMERR: u32 = 1 << 13;
const RXE_BREAK: u32 = 1 << 10;

impl State {
    /// Create a new state instance
    pub const fn new() -> Self {
        Self {
            rx_buf: RingBuffer::new(),
            tx_buf: RingBuffer::new(),
            rx_waker: AtomicWaker::new(),
            tx_waker: AtomicWaker::new(),
            rx_error: AtomicU32::new(0),
        }
    }
}

/// Buffered UART driver.
pub struct BufferedUart {
    pub(super) rx: BufferedUartRx,
    pub(super) tx: BufferedUartTx,
}

/// Buffered UART RX handle.
pub struct BufferedUartRx {
    pub(super) info: &'static Info,
    pub(super) state: &'static State,
    _flexcomm: FlexcommRef,
}

/// Buffered UART TX handle.
pub struct BufferedUartTx {
    pub(super) info: &'static Info,
    pub(super) state: &'static State,
    _flexcomm: FlexcommRef,
}

pub(super) fn init_buffers<'d>(
    info: &Info,
    state: &State,
    tx_buffer: Option<&'d mut [u8]>,
    rx_buffer: Option<&'d mut [u8]>,
) {
    if let Some(tx_buffer) = tx_buffer {
        let len = tx_buffer.len();
        unsafe { state.tx_buf.init(tx_buffer.as_mut_ptr(), len) };
    }

    if let Some(rx_buffer) = rx_buffer {
        let len = rx_buffer.len();
        unsafe { state.rx_buf.init(rx_buffer.as_mut_ptr(), len) };
    }

    // From the datasheet:
    // "The transmit interrupt is based on a transition through a level, rather
    // than on the level itself. When the interrupt and the UART is enabled
    // before any data is written to the transmit FIFO the interrupt is not set.
    // The interrupt is only set, after written data leaves the single location
    // of the transmit FIFO and it becomes empty."
    //
    // This means we can leave the interrupt enabled the whole time as long as
    // we clear it after it happens. The downside is that the we manually have
    // to pend the ISR when we want data transmission to start.
    info.regs().intenset().write(|w| {
        w.framerren()
            .set_bit()
            .parityerren()
            .set_bit()
            .rxnoiseen()
            .set_bit()
            .aberren()
            .set_bit()
    });

    info.regs()
        .fifointenset()
        .write(|w| w.rxlvl().set_bit().txlvl().set_bit());

    info.regs()
        .fifotrig()
        .write(|w| w.rxlvlena().set_bit().txlvlena().set_bit());

    info.interrupt.unpend();
    unsafe { info.interrupt.enable() };
}

impl BufferedUart {
    /// Create a buffered UART instance.
    pub fn new<'d, T: Instance>(
        _uart: Peri<'d, T>,
        tx: Peri<'d, impl TxPin<T>>,
        rx: Peri<'d, impl RxPin<T>>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx_buffer: &'d mut [u8],
        rx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        tx.as_tx();
        rx.as_rx();

        let flexcomm = super::Uart::<Async>::init::<T>(Some(tx.into()), Some(rx.into()), None, None, config)?;
        init_buffers(T::info(), T::buffered_state(), Some(tx_buffer), Some(rx_buffer));

        Ok(Self {
            rx: BufferedUartRx {
                info: T::info(),
                state: T::buffered_state(),
                _flexcomm: flexcomm.clone(),
            },
            tx: BufferedUartTx {
                info: T::info(),
                state: T::buffered_state(),
                _flexcomm: flexcomm,
            },
        })
    }

    /// Create a buffered UART instance with flow control.
    pub fn new_with_rtscts<'d, T: Instance>(
        _uart: Peri<'d, T>,
        tx: Peri<'d, impl TxPin<T>>,
        rx: Peri<'d, impl RxPin<T>>,
        rts: Peri<'d, impl RtsPin<T>>,
        cts: Peri<'d, impl CtsPin<T>>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx_buffer: &'d mut [u8],
        rx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        tx.as_tx();
        rx.as_rx();
        rts.as_rts();
        cts.as_cts();

        let flexcomm = super::Uart::<Async>::init::<T>(
            Some(tx.into()),
            Some(rx.into()),
            Some(rts.into()),
            Some(cts.into()),
            config,
        )?;
        init_buffers(T::info(), T::buffered_state(), Some(tx_buffer), Some(rx_buffer));

        Ok(Self {
            rx: BufferedUartRx {
                info: T::info(),
                state: T::buffered_state(),
                _flexcomm: flexcomm.clone(),
            },
            tx: BufferedUartTx {
                info: T::info(),
                state: T::buffered_state(),
                _flexcomm: flexcomm,
            },
        })
    }

    /// Write to UART TX buffer blocking execution until done.
    pub fn blocking_write(&mut self, buffer: &[u8]) -> Result<usize> {
        self.tx.blocking_write(buffer)
    }

    /// Flush UART TX blocking execution until done.
    pub fn blocking_flush(&mut self) -> Result<()> {
        self.tx.blocking_flush()
    }

    /// Read from UART RX buffer blocking execution until done.
    pub fn blocking_read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        self.rx.blocking_read(buffer)
    }

    /// Check if UART is busy transmitting.
    pub fn busy(&self) -> bool {
        self.tx.busy()
    }

    /// Split into separate RX and TX handles.
    pub fn split(self) -> (BufferedUartTx, BufferedUartRx) {
        (self.tx, self.rx)
    }

    /// Split the Uart into a transmitter and receiver by mutable reference,
    /// which is particularly useful when having two tasks correlating to
    /// transmitting and receiving.
    pub fn split_ref(&mut self) -> (&mut BufferedUartTx, &mut BufferedUartRx) {
        (&mut self.tx, &mut self.rx)
    }
}

impl BufferedUartRx {
    /// Create a new buffered UART RX.
    pub fn new<'d, T: Instance>(
        _uart: Peri<'d, T>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        rx: Peri<'d, impl RxPin<T>>,
        rx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        rx.as_rx();

        let _flexcomm = super::Uart::<Async>::init::<T>(None, Some(rx.into().reborrow()), None, None, config)?;
        init_buffers(T::info(), T::buffered_state(), None, Some(rx_buffer));

        Ok(Self {
            info: T::info(),
            state: T::buffered_state(),
            _flexcomm,
        })
    }

    /// Create a new buffered UART RX with flow control.
    pub fn new_with_rts<'d, T: Instance>(
        _uart: Peri<'d, T>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        rx: Peri<'d, impl RxPin<T>>,
        rts: Peri<'d, impl RtsPin<T>>,
        rx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        rx.as_rx();
        rts.as_rts();

        let _flexcomm =
            super::Uart::<Async>::init::<T>(None, Some(rx.into().reborrow()), Some(rts.into()), None, config)?;
        init_buffers(T::info(), T::buffered_state(), None, Some(rx_buffer));

        Ok(Self {
            info: T::info(),
            state: T::buffered_state(),
            _flexcomm,
        })
    }

    fn read<'a>(
        info: &'static Info,
        state: &'static State,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize>> + 'a {
        poll_fn(move |cx| {
            if let Poll::Ready(r) = Self::try_read(info, state, buf) {
                return Poll::Ready(r);
            }
            state.rx_waker.register(cx.waker());
            Poll::Pending
        })
    }

    fn get_rx_error(state: &State) -> Option<Error> {
        let errs = critical_section::with(|_| {
            let val = state.rx_error.load(Ordering::Relaxed);
            state.rx_error.store(0, Ordering::Relaxed);
            val
        });

        if errs & RXE_NOISE != 0 {
            Some(Error::Noise)
        } else if errs & RXE_PARITYERR != 0 {
            Some(Error::Parity)
        } else if errs & RXE_FRAMERR != 0 {
            Some(Error::Framing)
        } else if errs & RXE_BREAK != 0 {
            Some(Error::Break)
        } else {
            None
        }
    }

    fn try_read(info: &Info, state: &State, buf: &mut [u8]) -> Poll<Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut rx_reader = unsafe { state.rx_buf.reader() };
        let n = rx_reader.pop(|data| {
            let n = data.len().min(buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            n
        });

        let result = if n == 0 {
            match Self::get_rx_error(state) {
                None => return Poll::Pending,
                Some(e) => Err(e),
            }
        } else {
            Ok(n)
        };

        // (Re-)Enable the interrupt to receive more data in case it was
        // disabled because the buffer was full or errors were detected.
        info.regs().fifointenset().write(|w| w.rxlvl().set_bit());

        Poll::Ready(result)
    }

    /// Read from UART RX buffer blocking execution until done.
    pub fn blocking_read(&mut self, buf: &mut [u8]) -> Result<usize> {
        loop {
            match Self::try_read(self.info, self.state, buf) {
                Poll::Ready(res) => return res,
                Poll::Pending => continue,
            }
        }
    }

    fn fill_buf<'a>(state: &'static State) -> impl Future<Output = Result<&'a [u8]>> {
        poll_fn(move |cx| {
            let mut rx_reader = unsafe { state.rx_buf.reader() };
            let (p, n) = rx_reader.pop_buf();
            let result = if n == 0 {
                match Self::get_rx_error(state) {
                    None => {
                        state.rx_waker.register(cx.waker());
                        return Poll::Pending;
                    }
                    Some(e) => Err(e),
                }
            } else {
                let buf = unsafe { slice::from_raw_parts(p, n) };
                Ok(buf)
            };

            Poll::Ready(result)
        })
    }

    fn consume(info: &Info, state: &State, amt: usize) {
        let mut rx_reader = unsafe { state.rx_buf.reader() };
        rx_reader.pop_done(amt);

        // (Re-)Enable the interrupt to receive more data in case it was
        // disabled because the buffer was full or errors were detected.
        info.regs().fifointenset().write(|w| w.rxlvl().set_bit());
    }

    /// we are ready to read if there is data in the buffer
    fn read_ready(state: &State) -> Result<bool> {
        Ok(!state.rx_buf.is_empty())
    }
}

impl BufferedUartTx {
    /// Create a new buffered UART TX.
    pub fn new<'d, T: Instance>(
        _uart: Peri<'d, T>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx: Peri<'d, impl TxPin<T>>,
        tx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        tx.as_tx();

        let _flexcomm = super::Uart::<Async>::init::<T>(Some(tx.into().reborrow()), None, None, None, config)?;
        init_buffers(T::info(), T::buffered_state(), Some(tx_buffer), None);

        Ok(Self {
            info: T::info(),
            state: T::buffered_state(),
            _flexcomm,
        })
    }

    /// Create a new buffered UART TX with flow control.
    pub fn new_with_cts<'d, T: Instance>(
        _uart: Peri<'d, T>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx: Peri<'d, impl TxPin<T>>,
        cts: Peri<'d, impl CtsPin<T>>,
        tx_buffer: &'d mut [u8],
        config: Config,
    ) -> Result<Self> {
        tx.as_tx();
        cts.as_cts();

        let _flexcomm =
            super::Uart::<Async>::init::<T>(Some(tx.into().reborrow()), None, None, Some(cts.into()), config)?;
        init_buffers(T::info(), T::buffered_state(), Some(tx_buffer), None);

        Ok(Self {
            info: T::info(),
            state: T::buffered_state(),
            _flexcomm,
        })
    }

    fn write<'d>(
        info: &'static Info,
        state: &'static State,
        buf: &'d [u8],
    ) -> impl Future<Output = Result<usize>> + 'd {
        poll_fn(move |cx| {
            if buf.is_empty() {
                return Poll::Ready(Ok(0));
            }

            let mut tx_writer = unsafe { state.tx_buf.writer() };
            let n = tx_writer.push(|data| {
                let n = data.len().min(buf.len());
                data[..n].copy_from_slice(&buf[..n]);
                n
            });
            if n == 0 {
                state.tx_waker.register(cx.waker());
                return Poll::Pending;
            }

            // The TX interrupt only triggers when the there was data in the
            // FIFO and the number of bytes drops below a threshold. When the
            // FIFO was empty we have to manually pend the interrupt to shovel
            // TX data from the buffer into the FIFO.
            info.interrupt.pend();
            Poll::Ready(Ok(n))
        })
    }

    fn flush(state: &'static State) -> impl Future<Output = Result<()>> {
        poll_fn(move |cx| {
            if !state.tx_buf.is_empty() {
                state.tx_waker.register(cx.waker());
                return Poll::Pending;
            }

            Poll::Ready(Ok(()))
        })
    }

    /// Write to UART TX buffer blocking execution until done.
    pub fn blocking_write(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            let mut tx_writer = unsafe { self.state.tx_buf.writer() };
            let n = tx_writer.push(|data| {
                let n = data.len().min(buf.len());
                data[..n].copy_from_slice(&buf[..n]);
                n
            });

            if n != 0 {
                // The TX interrupt only triggers when the there was data in the
                // FIFO and the number of bytes drops below a threshold. When the
                // FIFO was empty we have to manually pend the interrupt to shovel
                // TX data from the buffer into the FIFO.
                self.info.interrupt.pend();
                return Ok(n);
            }
        }
    }

    /// Flush UART TX blocking execution until done.
    pub fn blocking_flush(&mut self) -> Result<()> {
        loop {
            if self.state.tx_buf.is_empty() {
                return Ok(());
            }
        }
    }

    /// Check if UART is busy.
    pub fn busy(&self) -> bool {
        self.info.regs().stat().read().txidle().bit_is_clear()
    }
}

impl Drop for BufferedUartRx {
    fn drop(&mut self) {
        unsafe { self.state.rx_buf.deinit() }

        // TX is inactive if the buffer is not available.
        // We can now unregister the interrupt handler
        if !self.state.tx_buf.is_available() {
            self.info.interrupt.disable();
        }
    }
}

impl Drop for BufferedUartTx {
    fn drop(&mut self) {
        unsafe { self.state.tx_buf.deinit() }

        // RX is inactive if the buffer is not available.
        // We can now unregister the interrupt handler
        if !self.state.rx_buf.is_available() {
            self.info.interrupt.disable();
        }
    }
}

/// Interrupt handler.
pub struct BufferedInterruptHandler<T: Instance> {
    _uart: PhantomData<T>,
}

impl<T: Instance> interrupt::typelevel::Handler<T::Interrupt> for BufferedInterruptHandler<T> {
    unsafe fn on_interrupt() {
        let regs = T::info().regs();
        let stat = regs.intstat().read();

        // if r.uartdmacr().read().rxdmae() {
        //     return;
        // }

        let s = T::buffered_state();

        // Clear TX and error interrupt flags
        if stat.txidle().bit_is_set()
            || stat.framerrint().bit_is_set()
            || stat.parityerrint().bit_is_set()
            || stat.rxnoiseint().bit_is_set()
            || stat.aberrint().bit_is_set()
        {
            regs.intenclr().write(|w| {
                w.txidleclr()
                    .set_bit()
                    .framerrclr()
                    .set_bit()
                    .parityerrclr()
                    .set_bit()
                    .rxnoiseclr()
                    .set_bit()
                    .aberrclr()
                    .set_bit()
            });
        }

        // Errors
        if stat.framerrint().bit_is_set() {
            warn!("Framing error");
        }
        if stat.parityerrint().bit_is_set() {
            warn!("Parity error");
        }
        if stat.rxnoiseint().bit_is_set() {
            warn!("Noise error");
        }
        if stat.deltarxbrk().bit_is_set() {
            warn!("Break error");
        }

        if regs.fifointstat().read().txlvl().bit_is_set() || regs.fifointstat().read().txerr().bit_is_set() {
            regs.fifointenclr().write(|w| w.txlvl().set_bit().txerr().set_bit());
        }

        if regs.fifointstat().read().rxlvl().bit_is_set() || regs.fifointstat().read().rxerr().bit_is_set() {
            regs.fifointenclr().write(|w| w.rxlvl().set_bit().rxerr().set_bit());
        }

        // RX
        if s.rx_buf.is_available() {
            let mut rx_writer = unsafe { s.rx_buf.writer() };
            let rx_buf = rx_writer.push_slice();
            let mut n_read = 0;
            let mut error = false;
            for rx_byte in rx_buf {
                if regs.fifostat().read().rxnotempty().bit_is_clear() {
                    // RX FIFO is empty, stop.
                    break;
                }
                let stat = regs.stat().read().bits();
                if (stat & (RXE_BREAK | RXE_FRAMERR | RXE_NOISE | RXE_PARITYERR)) != 0 {
                    critical_section::with(|_| {
                        let val = s.rx_error.load(Ordering::Relaxed);
                        s.rx_error.store(val | stat, Ordering::Relaxed);
                    });
                    error = true;
                    // only fill the buffer with valid characters. the current character is fine
                    // if the error is an overrun, but if we add it to the buffer we'll report
                    // the overrun one character too late. drop it instead and pretend we were
                    // a bit slower at draining the rx fifo than we actually were.
                    // this is consistent with blocking uart error reporting.
                    break;
                }
                *rx_byte = regs.fiford().read().rxdata().bits() as u8;
                n_read += 1;
            }
            if n_read > 0 {
                rx_writer.push_done(n_read);
                s.rx_waker.wake();
            } else if error {
                s.rx_waker.wake();
            }
            // Disable any further RX interrupts when the buffer becomes full or
            // errors have occurred. This lets us buffer additional errors in the
            // fifo without needing more error storage locations, and most applications
            // will want to do a full reset of their uart state anyway once an error
            // has happened.
            if s.rx_buf.is_full() || error {
                regs.fifointenclr().write(|w| w.rxlvl().set_bit());
            }
        }

        // TX
        if s.tx_buf.is_available() {
            let mut tx_reader = unsafe { s.tx_buf.reader() };
            let tx_buf = tx_reader.pop_slice();
            let mut n_written = 0;
            for tx_byte in tx_buf.iter_mut() {
                if regs.fifostat().read().txnotfull().bit_is_clear() {
                    // FIFO is full, stop.
                    break;
                }
                regs.fifowr().write(|w| unsafe { w.txdata().bits(u16::from(*tx_byte)) });
                n_written += 1;
            }
            if n_written > 0 {
                tx_reader.pop_done(n_written);
                s.tx_waker.wake();
            }
            // The TX interrupt only triggers once when the FIFO threshold is
            // crossed. No need to disable it when the buffer becomes empty
            // as it does re-trigger anymore once we have cleared it.
        }
    }
}

impl embedded_io_async::ErrorType for BufferedUart {
    type Error = Error;
}

impl embedded_io_async::ErrorType for BufferedUartRx {
    type Error = Error;
}

impl embedded_io_async::ErrorType for BufferedUartTx {
    type Error = Error;
}

impl embedded_io_async::Read for BufferedUart {
    async fn read(&mut self, buf: &mut [u8]) -> core::result::Result<usize, Self::Error> {
        BufferedUartRx::read(self.rx.info, self.rx.state, buf).await
    }
}

impl embedded_io_async::Read for BufferedUartRx {
    async fn read(&mut self, buf: &mut [u8]) -> core::result::Result<usize, Self::Error> {
        Self::read(self.info, self.state, buf).await
    }
}

impl embedded_io_async::ReadReady for BufferedUart {
    fn read_ready(&mut self) -> core::result::Result<bool, Self::Error> {
        BufferedUartRx::read_ready(self.rx.state)
    }
}

impl embedded_io_async::ReadReady for BufferedUartRx {
    fn read_ready(&mut self) -> core::result::Result<bool, Self::Error> {
        Self::read_ready(self.state)
    }
}

impl embedded_io_async::BufRead for BufferedUart {
    async fn fill_buf(&mut self) -> core::result::Result<&[u8], Self::Error> {
        BufferedUartRx::fill_buf(self.rx.state).await
    }

    fn consume(&mut self, amt: usize) {
        BufferedUartRx::consume(self.rx.info, self.rx.state, amt)
    }
}

impl embedded_io_async::BufRead for BufferedUartRx {
    async fn fill_buf(&mut self) -> core::result::Result<&[u8], Self::Error> {
        Self::fill_buf(self.state).await
    }

    fn consume(&mut self, amt: usize) {
        Self::consume(self.info, self.state, amt)
    }
}

impl embedded_io_async::Write for BufferedUart {
    async fn write(&mut self, buf: &[u8]) -> core::result::Result<usize, Self::Error> {
        BufferedUartTx::write(self.tx.info, self.tx.state, buf).await
    }

    async fn flush(&mut self) -> core::result::Result<(), Self::Error> {
        BufferedUartTx::flush(self.tx.state).await
    }
}

impl embedded_io_async::Write for BufferedUartTx {
    async fn write(&mut self, buf: &[u8]) -> core::result::Result<usize, Self::Error> {
        Self::write(self.info, self.state, buf).await
    }

    async fn flush(&mut self) -> core::result::Result<(), Self::Error> {
        Self::flush(self.state).await
    }
}

impl embedded_io::Read for BufferedUart {
    fn read(&mut self, buf: &mut [u8]) -> core::result::Result<usize, Self::Error> {
        self.rx.blocking_read(buf)
    }
}

impl embedded_io::Read for BufferedUartRx {
    fn read(&mut self, buf: &mut [u8]) -> core::result::Result<usize, Self::Error> {
        self.blocking_read(buf)
    }
}

impl embedded_io::Write for BufferedUart {
    fn write(&mut self, buf: &[u8]) -> core::result::Result<usize, Self::Error> {
        self.tx.blocking_write(buf)
    }

    fn flush(&mut self) -> core::result::Result<(), Self::Error> {
        self.tx.blocking_flush()
    }
}

impl embedded_io::Write for BufferedUartTx {
    fn write(&mut self, buf: &[u8]) -> core::result::Result<usize, Self::Error> {
        self.blocking_write(buf)
    }

    fn flush(&mut self) -> core::result::Result<(), Self::Error> {
        self.blocking_flush()
    }
}

impl embedded_hal_02::serial::Read<u8> for BufferedUartRx {
    type Error = Error;

    fn read(&mut self) -> core::result::Result<u8, nb::Error<Self::Error>> {
        if self.info.regs().fifostat().read().rxnotempty().bit_is_clear() {
            Err(nb::Error::WouldBlock)
        } else if self.info.regs().fifostat().read().rxerr().bit_is_set() {
            self.info.regs().fifocfg().modify(|_, w| w.emptyrx().set_bit());
            self.info.regs().fifostat().modify(|_, w| w.rxerr().set_bit());
            Err(nb::Error::Other(Error::Read))
        } else if self.info.regs().stat().read().parityerrint().bit_is_set() {
            self.info
                .regs()
                .stat()
                .modify(|_, w| w.parityerrint().clear_bit_by_one());
            Err(nb::Error::Other(Error::Parity))
        } else if self.info.regs().stat().read().framerrint().bit_is_set() {
            self.info.regs().stat().modify(|_, w| w.framerrint().clear_bit_by_one());
            Err(nb::Error::Other(Error::Framing))
        } else if self.info.regs().stat().read().rxnoiseint().bit_is_set() {
            self.info.regs().stat().modify(|_, w| w.rxnoiseint().clear_bit_by_one());
            Err(nb::Error::Other(Error::Noise))
        } else {
            let byte = self.info.regs().fiford().read().rxdata().bits() as u8;
            Ok(byte)
        }
    }
}

impl embedded_hal_02::blocking::serial::Write<u8> for BufferedUartTx {
    type Error = Error;

    fn bwrite_all(&mut self, mut buffer: &[u8]) -> core::result::Result<(), Self::Error> {
        while !buffer.is_empty() {
            match self.blocking_write(buffer) {
                Ok(0) => panic!("zero-length write."),
                Ok(n) => buffer = &buffer[n..],
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn bflush(&mut self) -> core::result::Result<(), Self::Error> {
        self.blocking_flush()
    }
}

impl embedded_hal_02::serial::Read<u8> for BufferedUart {
    type Error = Error;

    fn read(&mut self) -> core::result::Result<u8, nb::Error<Self::Error>> {
        embedded_hal_02::serial::Read::read(&mut self.rx)
    }
}

impl embedded_hal_02::blocking::serial::Write<u8> for BufferedUart {
    type Error = Error;

    fn bwrite_all(&mut self, mut buffer: &[u8]) -> core::result::Result<(), Self::Error> {
        while !buffer.is_empty() {
            match self.blocking_write(buffer) {
                Ok(0) => panic!("zero-length write."),
                Ok(n) => buffer = &buffer[n..],
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn bflush(&mut self) -> core::result::Result<(), Self::Error> {
        self.blocking_flush()
    }
}

impl embedded_hal_nb::serial::ErrorType for BufferedUartRx {
    type Error = Error;
}

impl embedded_hal_nb::serial::ErrorType for BufferedUartTx {
    type Error = Error;
}

impl embedded_hal_nb::serial::ErrorType for BufferedUart {
    type Error = Error;
}

impl embedded_hal_nb::serial::Read for BufferedUartRx {
    fn read(&mut self) -> nb::Result<u8, Self::Error> {
        embedded_hal_02::serial::Read::read(self)
    }
}

impl embedded_hal_nb::serial::Write for BufferedUartTx {
    fn write(&mut self, char: u8) -> nb::Result<(), Self::Error> {
        self.blocking_write(&[char]).map(drop).map_err(nb::Error::Other)
    }

    fn flush(&mut self) -> nb::Result<(), Self::Error> {
        self.blocking_flush().map_err(nb::Error::Other)
    }
}

impl embedded_hal_nb::serial::Read for BufferedUart {
    fn read(&mut self) -> core::result::Result<u8, nb::Error<Self::Error>> {
        embedded_hal_02::serial::Read::read(&mut self.rx)
    }
}

impl embedded_hal_nb::serial::Write for BufferedUart {
    fn write(&mut self, char: u8) -> nb::Result<(), Self::Error> {
        self.blocking_write(&[char]).map(drop).map_err(nb::Error::Other)
    }

    fn flush(&mut self) -> nb::Result<(), Self::Error> {
        self.blocking_flush().map_err(nb::Error::Other)
    }
}
