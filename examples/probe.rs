//! Debug probe: mirrors audio.rs device selection and prints exactly which
//! device/config cpal picks and where stream building fails.
//! Usage: cargo run --example probe [name-substring]
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{FromSample, Sample, SampleFormat, SizedSample};

fn main() {
    let host = cpal::default_host();
    let wanted = std::env::args().nth(1).unwrap_or_else(|| "pwconv".into());
    let devs: Vec<_> = host.input_devices().expect("enumerate").collect();
    println!("== enumerated input devices ==");
    for d in &devs {
        println!("  {:?}", d.name().unwrap_or_default());
    }
    let device = devs
        .into_iter()
        .find(|d| d.name().is_ok_and(|n| n.contains(&wanted)))
        .or_else(|| host.default_input_device())
        .expect("no audio input device");
    println!("picked: {:?}", device.name().unwrap_or_default());
    let supported = device.default_input_config();
    println!("default_input_config: {supported:?}");
    let supported = supported.expect("no default input config");
    let config: cpal::StreamConfig = supported.clone().into();
    println!("stream config: {config:?}");
    let stream = match supported.sample_format() {
        SampleFormat::F32 => build::<f32>(&device, &config),
        SampleFormat::I16 => build::<i16>(&device, &config),
        SampleFormat::U16 => build::<u16>(&device, &config),
        SampleFormat::I32 => build::<i32>(&device, &config),
        other => panic!("unsupported sample format {other:?}"),
    };
    let stream = stream.expect("failed to build input stream");
    stream.play().expect("play");
    println!("stream playing OK, capturing 2s of silence…");
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("done, no errors");
}

fn build<T: SizedSample>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
) -> Result<cpal::Stream, cpal::BuildStreamError>
where
    f32: FromSample<T>,
    T: Sample,
{
    device.build_input_stream(
        config,
        move |_data: &[T], _| {},
        |e| eprintln!("stream error: {e}"),
        None,
    )
}
