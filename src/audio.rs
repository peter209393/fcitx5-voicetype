//! Microphone capture. Delivers 16 kHz mono s16le PCM chunks (the format
//! the recognizer wants) on a channel; the stream stops when dropped.

use anyhow::{bail, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{FromSample, Sample, SampleFormat, SizedSample};
use tokio::sync::mpsc::UnboundedSender;

pub const TARGET_RATE: u32 = 16_000;

/// Opens the input device (name substring match; empty = "pipewire" if
/// present, else the host default) and starts capturing into `tx`.
pub fn start(device_name: &str, tx: UnboundedSender<Vec<u8>>) -> Result<cpal::Stream> {
    let host = cpal::default_host();
    let wanted = if device_name.is_empty() {
        "pipewire"
    } else {
        device_name
    };
    let device = host
        .input_devices()
        .ok()
        .and_then(|mut devs| devs.find(|d| d.name().is_ok_and(|n| n.contains(wanted))))
        .or_else(|| host.default_input_device())
        .context("no audio input device")?;
    let supported = device
        .default_input_config()
        .context("no default input config")?;
    let config: cpal::StreamConfig = supported.clone().into();

    let stream = match supported.sample_format() {
        SampleFormat::F32 => build::<f32>(&device, &config, tx)?,
        SampleFormat::I16 => build::<i16>(&device, &config, tx)?,
        SampleFormat::U16 => build::<u16>(&device, &config, tx)?,
        SampleFormat::I32 => build::<i32>(&device, &config, tx)?,
        other => bail!("unsupported sample format {other:?}"),
    };
    stream.play().context("failed to start input stream")?;
    Ok(stream)
}

fn build<T: SizedSample>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    tx: UnboundedSender<Vec<u8>>,
) -> Result<cpal::Stream>
where
    f32: FromSample<T>,
{
    let channels = config.channels.max(1) as usize;
    let rate = config.sample_rate.0;
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                let mono: Vec<f32> = data
                    .chunks(channels)
                    .map(|f| f.iter().map(|&s| f32::from_sample(s)).sum::<f32>() / channels as f32)
                    .collect();
                let _ = tx.send(to_pcm16k(&mono, rate));
            },
            |e| eprintln!("[voicetype] audio stream error: {e}"),
            None,
        )
        .context("failed to build input stream")
}

/// Linear resample to 16 kHz and pack as little-endian i16.
pub fn to_pcm16k(samples: &[f32], from_rate: u32) -> Vec<u8> {
    let ratio = TARGET_RATE as f64 / from_rate as f64;
    let n = (samples.len() as f64 * ratio) as usize;
    let mut out = Vec::with_capacity(n * 2);
    for i in 0..n {
        let pos = i as f64 / ratio;
        let idx = pos as usize;
        let frac = (pos - idx as f64) as f32;
        let s0 = samples.get(idx).copied().unwrap_or(0.0);
        let s1 = samples.get(idx + 1).copied().unwrap_or(s0);
        let s = s0 * (1.0 - frac) + s1 * frac;
        out.extend_from_slice(&((s.clamp(-1.0, 1.0) * i16::MAX as f32) as i16).to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resamples_and_packs() {
        let pcm = to_pcm16k(&[0.0; 480], 48_000);
        assert_eq!(pcm.len(), 160 * 2);
        let pcm = to_pcm16k(&[1.0, 1.0], 16_000);
        assert_eq!(pcm, [0xff, 0x7f, 0xff, 0x7f]);
    }
}
