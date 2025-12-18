#![no_std]
#![no_main]

use defmt::info;
use embassy_executor::Spawner;
use embassy_imxrt::uart::buffered::{BufferedInterruptHandler, BufferedUart, BufferedUartRx};
use embassy_imxrt::{bind_interrupts, peripherals};
use embassy_time::Timer;
use embedded_io_async::{Read, Write};
use static_cell::StaticCell;
use {defmt_rtt as _, embassy_imxrt_examples as _, panic_probe as _};

bind_interrupts!(struct Irqs {
    FLEXCOMM4 => BufferedInterruptHandler<peripherals::FLEXCOMM4>;
});

const BUFLEN: usize = 32;

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    let p = embassy_imxrt::init(Default::default());

    info!("Buffered UART");

    static TX_BUF: StaticCell<[u8; BUFLEN]> = StaticCell::new();
    let tx_buf = &mut TX_BUF.init([0; BUFLEN])[..];
    static RX_BUF: StaticCell<[u8; BUFLEN]> = StaticCell::new();
    let rx_buf = &mut RX_BUF.init([0; BUFLEN])[..];

    let uart = BufferedUart::new(
        p.FLEXCOMM4,
        p.PIO0_29,
        p.PIO0_30,
        Irqs,
        tx_buf,
        rx_buf,
        // p.DMA0_CH5,
        // p.DMA0_CH4,
        Default::default(),
    )
    .unwrap();
    let (mut tx, rx) = uart.split();

    spawner.must_spawn(reader(rx));

    info!("Writing...");
    loop {
        let data = [
            1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31,
        ];
        info!("TX {:?}", data);
        tx.write_all(&data).await.unwrap();
        Timer::after_secs(1).await;
    }
}

#[embassy_executor::task]
async fn reader(mut rx: BufferedUartRx) {
    info!("Reading...");
    loop {
        let mut buf = [0; 31];
        rx.read_exact(&mut buf).await.unwrap();
        info!("RX {:?}", buf);
    }
}
