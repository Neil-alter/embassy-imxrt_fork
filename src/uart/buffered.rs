use crate::dma::channel::Channel;
use core::future::poll_fn;
use core::marker::PhantomData;
use core::task::Poll;
use embassy_futures::select::{Either, select};
use embassy_hal_internal::Peri;
use embassy_hal_internal::atomic_ring_buffer::RingBuffer;

use super::{Async, Config, CtsPin, Error, Info, Instance, Result, RtsPin, RxDma, RxPin, TxDma, TxPin, Uart, UartTx};
use crate::dma::transfer;
use crate::interrupt::typelevel::Interrupt;
use crate::{dma, interrupt};

struct BufferConfig {
    dma_buf: &'static mut [u8],
    dma_last_write_index: usize,
    polling_rate: u64,
}

pub(super) struct BufferedUartState {
    pub(super) ring_buf: RingBuffer,
}

pub(super) fn init_buffers(state: &BufferedUartState, ring_buffer: Option<&'static mut [u8]>) {
    if let Some(ring_buffer) = ring_buffer {
        let len = ring_buffer.len();
        unsafe { state.ring_buf.init(ring_buffer.as_mut_ptr(), len) };
    }
}

pub struct BufferedInterruptHandler<T: Instance> {
    _uart: PhantomData<T>,
}

impl<T: Instance> interrupt::typelevel::Handler<T::Interrupt> for BufferedInterruptHandler<T> {
    unsafe fn on_interrupt() {
        let regs = T::info().regs;
        let stat = regs.intstat().read();

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
            T::waker().wake();
        }
        let fifostat = regs.fifointstat().read();
        if fifostat.txlvl().bit_is_set() || fifostat.txerr().bit_is_set() {
            regs.fifointenclr().write(|w| w.txlvl().set_bit().txerr().set_bit());
        }
        if fifostat.rxlvl().bit_is_set() || fifostat.rxerr().bit_is_set() {
            regs.fifointenclr().write(|w| w.rxlvl().set_bit().rxerr().set_bit());
        }
    }
}

/// Buffered Uart driver.
pub struct BufferedUart<'a> {
    info: Info,
    tx: UartTx<'a, Async>,
    rx: BufferedUartRx<'a>,
}

impl<'a> BufferedUart<'a> {
    /// Create a new buffered UART with DMA support.
    pub fn new<T: Instance>(
        _inner: Peri<'a, T>,
        tx: Peri<'a, impl TxPin<T>>,
        rx: Peri<'a, impl RxPin<T>>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx_dma: Peri<'a, impl TxDma<T>>,
        rx_dma: Peri<'a, impl RxDma<T>>,
        config: Config,
        rx_buffer: &'static mut [u8],
        polling_rate_us: u64,
    ) -> Result<Self> {
        if rx_buffer.len() > 1024 {
            return Err(Error::InvalidArgument);
        }
        let mid = rx_buffer.len() / 2;
        if mid == 0 {
            return Err(Error::InvalidArgument);
        }
        let (dma_buf, ring_buffer) = rx_buffer.split_at_mut(mid);

        tx.as_tx();
        rx.as_rx();

        let tx = tx.into();
        let rx = rx.into();

        T::Interrupt::unpend();
        unsafe { T::Interrupt::enable() };

        let tx_dma = dma::Dma::reserve_channel(tx_dma);
        let rx_dma: Channel<'_> = dma::Dma::reserve_channel(rx_dma).ok_or(Error::Fail)?;

        T::info().regs.fifocfg().modify(|_, w| w.dmarx().enabled());

        // Save buffer pointer and length before moving dma_buf
        let rx_ptr = dma_buf.as_mut_ptr();
        let rx_len = dma_buf.len();

        // immediately configure and enable channel for circular buffered reception
        rx_dma.configure_channel(
            dma::transfer::Direction::PeripheralToMemory,
            T::info().regs.fiford().as_ptr() as *const u8 as *const u32,
            rx_ptr as *mut u32,
            rx_len,
            dma::transfer::TransferOptions {
                width: dma::transfer::Width::Bit8,
                priority: dma::transfer::Priority::Priority0,
                mode: transfer::Mode::Continuous,
                trigger: transfer::Trigger::Software,
            },
        );
        rx_dma.enable_channel();
        rx_dma.trigger_channel();

        let flexcomm = Uart::<'a, Async>::init::<T>(Some(tx.into()), Some(rx.into()), None, None, config)?;
        init_buffers(T::buffered_state(), Some(ring_buffer));

        Ok(Self {
            info: T::info(),
            tx: UartTx::new_inner::<T>(flexcomm.clone(), tx_dma),
            rx: BufferedUartRx {
                info: T::info(),
                state: T::buffered_state(),
                _buffer_config: Some(BufferConfig {
                    dma_buf: dma_buf,
                    dma_last_write_index: 0,
                    polling_rate: polling_rate_us,
                }),
                _rx_dma: Some(rx_dma),
            },
        })
    }

    pub fn new_with_rtscts<T: Instance>(
        _inner: Peri<'a, T>,
        tx: Peri<'a, impl TxPin<T>>,
        rx: Peri<'a, impl RxPin<T>>,
        rts: Peri<'a, impl RtsPin<T>>,
        cts: Peri<'a, impl CtsPin<T>>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        tx_dma: Peri<'a, impl TxDma<T>>,
        rx_dma: Peri<'a, impl RxDma<T>>,
        config: Config,
        rx_buffer: &'static mut [u8],
        polling_rate_us: u64,
    ) -> Result<Self> {
        if rx_buffer.len() > 1024 {
            return Err(Error::InvalidArgument);
        }
        let mid = rx_buffer.len() / 2;
        if mid == 0 {
            return Err(Error::InvalidArgument);
        }
        let (dma_buf, ring_buffer) = rx_buffer.split_at_mut(mid);

        tx.as_tx();
        rx.as_rx();
        rts.as_rts();
        cts.as_cts();

        let tx = tx.into();
        let rx = rx.into();
        let rts = rts.into();
        let cts = cts.into();

        let tx_dma = dma::Dma::reserve_channel(tx_dma);
        let rx_dma: Channel<'_> = dma::Dma::reserve_channel(rx_dma).ok_or(Error::Fail)?;

        T::info().regs.fifocfg().modify(|_, w| w.dmarx().enabled());

        // Save buffer pointer and length before moving dma_buf
        let rx_ptr = dma_buf.as_mut_ptr();
        let rx_len = dma_buf.len();

        // immediately configure and enable channel for circular buffered reception
        rx_dma.configure_channel(
            dma::transfer::Direction::PeripheralToMemory,
            T::info().regs.fiford().as_ptr() as *const u8 as *const u32,
            rx_ptr as *mut u32,
            rx_len,
            dma::transfer::TransferOptions {
                width: dma::transfer::Width::Bit8,
                priority: dma::transfer::Priority::Priority0,
                mode: transfer::Mode::Continuous,
                trigger: transfer::Trigger::Software,
            },
        );
        rx_dma.enable_channel();
        rx_dma.trigger_channel();

        let flexcomm = Uart::<'a, Async>::init::<T>(
            Some(tx.into()),
            Some(rx.into()),
            Some(rts.into()),
            Some(cts.into()),
            config,
        )?;
        init_buffers(T::buffered_state(), Some(ring_buffer));

        Ok(Self {
            info: T::info(),
            tx: UartTx::new_inner::<T>(flexcomm.clone(), tx_dma),
            rx: BufferedUartRx {
                info: T::info(),
                state: T::buffered_state(),
                _buffer_config: Some(BufferConfig {
                    dma_buf: dma_buf,
                    dma_last_write_index: 0,
                    polling_rate: polling_rate_us,
                }),
                _rx_dma: Some(rx_dma),
            },
        })
    }

    pub fn split(self) -> (UartTx<'a, Async>, BufferedUartRx<'a>) {
        (self.tx, self.rx)
    }
}

/// Buffered Uart RX driver
pub struct BufferedUartRx<'a> {
    info: Info,
    state: &'static BufferedUartState,
    _buffer_config: Option<BufferConfig>,
    _rx_dma: Option<Channel<'a>>,
}

impl<'a> BufferedUartRx<'a> {
    /// Create a new buffered UART RX with DMA support.
    pub fn new<T: Instance>(
        _inner: Peri<'a, T>,
        rx: Peri<'a, impl RxPin<T>>,
        _irq: impl interrupt::typelevel::Binding<T::Interrupt, BufferedInterruptHandler<T>>,
        rx_dma: Peri<'a, impl RxDma<T>>,
        config: Config,
        rx_buffer: &'static mut [u8],
        polling_rate_us: u64,
    ) -> Result<Self> {
        if rx_buffer.len() > 1024 {
            return Err(Error::InvalidArgument);
        }
        let mid = rx_buffer.len() / 2;
        if mid == 0 {
            return Err(Error::InvalidArgument);
        }
        let (dma_buf, ring_buffer) = rx_buffer.split_at_mut(mid);

        rx.as_rx();

        let mut _rx = rx.into();

        T::Interrupt::unpend();
        unsafe { T::Interrupt::enable() };

        let rx_dma = dma::Dma::reserve_channel(rx_dma).ok_or(Error::Fail)?;
        T::info().regs.fifocfg().modify(|_, w| w.dmarx().enabled());

        // Save buffer pointer and length before moving dma_buf
        let rx_ptr = dma_buf.as_mut_ptr();
        let rx_len = dma_buf.len();

        // immediately configure and enable channel for circular buffered reception
        rx_dma.configure_channel(
            dma::transfer::Direction::PeripheralToMemory,
            T::info().regs.fiford().as_ptr() as *const u8 as *const u32,
            rx_ptr as *mut u32,
            rx_len,
            dma::transfer::TransferOptions {
                width: dma::transfer::Width::Bit8,
                priority: dma::transfer::Priority::Priority0,
                mode: transfer::Mode::Continuous,
                trigger: transfer::Trigger::Software,
            },
        );
        rx_dma.enable_channel();
        rx_dma.trigger_channel();

        let _flexcomm = Uart::<Async>::init::<T>(None, Some(_rx.reborrow()), None, None, config)?;
        init_buffers(T::buffered_state(), Some(ring_buffer));

        Ok(Self {
            info: T::info(),
            state: T::buffered_state(),
            _buffer_config: Some(BufferConfig {
                dma_buf: dma_buf,
                dma_last_write_index: 0,
                polling_rate: polling_rate_us,
            }),
            _rx_dma: Some(rx_dma),
        })
    }

    fn ring_push_all(ring: &RingBuffer, mut data: &[u8]) -> usize {
        let mut total_pushed = 0;

        while !data.is_empty() {
            let mut writer = unsafe { ring.writer() };
            let (ptr, n) = writer.push_buf();
            if n == 0 {
                break; // Ring buffer is full, stop pushing data until next polling
            }
            let to_copy = n.min(data.len());
            unsafe {
                core::ptr::copy_nonoverlapping(data.as_ptr(), ptr, to_copy);
            }
            writer.push_done(to_copy);

            total_pushed += to_copy;
            data = &data[to_copy..];
        }

        total_pushed
    }

    async fn read_buffered_new(&mut self, buf: &mut [u8]) -> Result<usize> {
        // unwrap safe here as only entry path to API requires rx_dma instance
        let rx_dma = self._rx_dma.as_ref().unwrap();
        let buffer_config = self._buffer_config.as_mut().unwrap();
        let ring_buffer = &self.state.ring_buf;

        let mut bytes_read = 0;
        let mut write_index: usize = 0;
        let mut read_index: usize = 0;

        // As the Rx Idle interrupt is not present for this processor, we must poll to see if new data is available
        loop {
            // If there is data in the ring buffer, read and return it
            {
                let mut reader = unsafe { ring_buffer.reader() };
                let (ptr, avail) = reader.pop_buf();

                if avail > 0 {
                    let n = avail.min(buf.len() - bytes_read);
                    let src = unsafe { core::slice::from_raw_parts(ptr, n) };
                    buf[bytes_read..bytes_read + n].copy_from_slice(src);
                    reader.pop_done(n);
                    bytes_read += n;
                    
                    if bytes_read >= buf.len() {
                        return Ok(bytes_read);
                    }
                }
            }
            if rx_dma.is_active() {
                let mut remaining_bytes = rx_dma.get_xfer_count() as usize;
                remaining_bytes += 1;

                if remaining_bytes > buffer_config.dma_buf.len() {
                    return Err(Error::InvalidArgument);
                }

                // determine current write index where transfer will continue to
                write_index = (buffer_config.dma_buf.len() - remaining_bytes) % buffer_config.dma_buf.len();
            }

            read_index = buffer_config.dma_last_write_index;

            if write_index != read_index {
                if write_index > read_index {
                    let slice = &buffer_config.dma_buf[read_index..write_index];
                    let pushed = Self::ring_push_all(ring_buffer, slice);
                    read_index = read_index + pushed;
                } else {
                    // handle roll over
                    let slice1 = &buffer_config.dma_buf[read_index..buffer_config.dma_buf.len()];
                    let pushed1 = Self::ring_push_all(ring_buffer, slice1);
                    read_index = (read_index + pushed1) % buffer_config.dma_buf.len();

                    // still have more to push from begining of buffer
                    if pushed1 == slice1.len() && write_index > 0 {
                        let slice2 = &buffer_config.dma_buf[0..write_index];
                        let pushed2 = Self::ring_push_all(ring_buffer, slice2);
                        read_index = pushed2;
                    }
                }
                buffer_config.dma_last_write_index = read_index;
            } else {
                // sleep until next polling epoch, or if we detect a there is data in ringbuffer or error event
                let res = select(
                    // use embassy_time to enable polling the bus for more data
                    embassy_time::Timer::after_micros(buffer_config.polling_rate),
                    // detect bus errors
                    poll_fn(|cx| {
                        self.info.waker.register(cx.waker());

                        self.info.regs.intenset().write(|w| {
                            w.framerren()
                                .set_bit()
                                .parityerren()
                                .set_bit()
                                .rxnoiseen()
                                .set_bit()
                                .aberren()
                                .set_bit()
                        });

                        let stat = self.info.regs.stat().read();

                        self.info.regs.stat().write(|w| {
                            w.framerrint()
                                .clear_bit_by_one()
                                .parityerrint()
                                .clear_bit_by_one()
                                .rxnoiseint()
                                .clear_bit_by_one()
                                .aberr()
                                .clear_bit_by_one()
                        });

                        if stat.framerrint().bit_is_set() {
                            Poll::Ready(Err(Error::Framing))
                        } else if stat.parityerrint().bit_is_set() {
                            Poll::Ready(Err(Error::Parity))
                        } else if stat.rxnoiseint().bit_is_set() {
                            Poll::Ready(Err(Error::Noise))
                        } else if stat.aberr().bit_is_set() {
                            Poll::Ready(Err(Error::Fail))
                        } else {
                            Poll::Pending
                        }
                    }),
                )
                .await;

                match res {
                    Either::First(()) => {}
                    Either::Second(Ok(())) => (),
                    Either::Second(Err(e)) => {
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl embedded_io_async::ErrorType for BufferedUartRx<'_> {
    type Error = Error;
}

impl embedded_io_async::Read for BufferedUartRx<'_> {
    async fn read(&mut self, buf: &mut [u8]) -> core::result::Result<usize, Self::Error> {
        self.read_buffered_new(buf).await
    }
}
