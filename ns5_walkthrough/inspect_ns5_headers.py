#!/usr/bin/env python3
"""Inspect an NS5 file header and explain how the data section is laid out."""

import argparse
import struct
from pathlib import Path
from typing import Dict, List, Tuple


NSX21_BASIC_FMT = "<16sII"
NSX22_BASIC_FMT = "<2BI16s256sII8HI"
NSX22_EXT_FMT = "<2sH16sBBhhhh16sIIHIIH"

NSX21_BASIC_SIZE = struct.calcsize(NSX21_BASIC_FMT)
NSX22_BASIC_SIZE = struct.calcsize(NSX22_BASIC_FMT)
NSX22_EXT_SIZE = struct.calcsize(NSX22_EXT_FMT)


def _strip_nulls(raw: bytes) -> str:
    return raw.decode("latin-1", errors="replace").split("\x00", 1)[0]


def parse_time_origin(fields: Tuple[int, ...]) -> str:
    year, month, _weekday, day, hour, minute, second, millisecond = fields
    return (
        f"{year:04d}-{month:02d}-{day:02d} "
        f"{hour:02d}:{minute:02d}:{second:02d}.{millisecond:03d}"
    )


def parse_nsx21(path: Path) -> Dict[str, object]:
    with path.open("rb") as f:
        file_type_id = _strip_nulls(f.read(8).ljust(8, b"\x00"))
        label_raw, period, channel_count = struct.unpack(NSX21_BASIC_FMT, f.read(NSX21_BASIC_SIZE))
        channel_ids = list(struct.unpack(f"<{channel_count}I", f.read(4 * channel_count)))

    sample_resolution = 30000
    sample_rate_hz = sample_resolution / float(period)
    bytes_in_header = 8 + NSX21_BASIC_SIZE + 4 * channel_count
    return {
        "file_type_id": file_type_id,
        "file_spec": "2.1",
        "label": _strip_nulls(label_raw),
        "period": period,
        "sample_resolution": sample_resolution,
        "sample_rate_hz": sample_rate_hz,
        "channel_count": channel_count,
        "bytes_in_header": bytes_in_header,
        "channel_ids": channel_ids,
    }


def parse_nsx22_or_newer(path: Path) -> Dict[str, object]:
    with path.open("rb") as f:
        file_type_id = _strip_nulls(f.read(8).ljust(8, b"\x00"))
        basic = struct.unpack(NSX22_BASIC_FMT, f.read(NSX22_BASIC_SIZE))
        major, minor = basic[0], basic[1]
        bytes_in_header = basic[2]
        label = _strip_nulls(basic[3])
        comment = _strip_nulls(basic[4])
        period = basic[5]
        sample_resolution = basic[6]
        time_origin = parse_time_origin(basic[7:15])
        channel_count = basic[15]
        use_u64_timestamps = major > 2

        extended_headers: List[Dict[str, object]] = []
        for _ in range(channel_count):
            fields = struct.unpack(NSX22_EXT_FMT, f.read(NSX22_EXT_SIZE))
            extended_headers.append(
                {
                    "type": _strip_nulls(fields[0]),
                    "electrode_id": fields[1],
                    "electrode_label": _strip_nulls(fields[2]),
                    "physical_connector": fields[3],
                    "connector_pin": fields[4],
                    "min_digital_value": fields[5],
                    "max_digital_value": fields[6],
                    "min_analog_value": fields[7],
                    "max_analog_value": fields[8],
                    "units": _strip_nulls(fields[9]),
                }
            )

        first_packet_offset = f.tell()
        packet_header_size = 13 if use_u64_timestamps else 9
        packet_header = f.read(packet_header_size)

    sample_rate_hz = sample_resolution / float(period)
    first_packet = None
    if len(packet_header) == packet_header_size:
        first_packet = {
            "header_byte": packet_header[0],
            "timestamp": (
                struct.unpack("<Q", packet_header[1:9])[0]
                if use_u64_timestamps
                else struct.unpack("<I", packet_header[1:5])[0]
            ),
            "num_data_points": (
                struct.unpack("<I", packet_header[9:13])[0]
                if use_u64_timestamps
                else struct.unpack("<I", packet_header[5:9])[0]
            ),
        }

    return {
        "file_type_id": file_type_id,
        "file_spec": f"{major}.{minor}",
        "label": label,
        "comment": comment,
        "period": period,
        "sample_resolution": sample_resolution,
        "sample_rate_hz": sample_rate_hz,
        "time_origin": time_origin,
        "channel_count": channel_count,
        "bytes_in_header": bytes_in_header,
        "extended_headers": extended_headers,
        "first_packet_offset": first_packet_offset,
        "first_packet_header_size": packet_header_size,
        "first_packet": first_packet,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ns5_path", help="Path to a .ns5 file")
    parser.add_argument(
        "--show-channels",
        type=int,
        default=16,
        help="How many channel entries to print from the header table.",
    )
    args = parser.parse_args()

    path = Path(args.ns5_path).resolve()
    with path.open("rb") as f:
        file_type_id = _strip_nulls(f.read(8).ljust(8, b"\x00"))

    if file_type_id == "NEURALSG":
        info = parse_nsx21(path)
        print(f"file: {path}")
        print(f"FileTypeID: {info['file_type_id']}")
        print(f"FileSpec: {info['file_spec']}")
        print(f"Label: {info['label']}")
        print(f"ChannelCount: {info['channel_count']}")
        print(f"Period: {info['period']}")
        print(f"SampleResolution: {info['sample_resolution']}")
        print(f"SampleRateHz: {info['sample_rate_hz']:.6f}")
        print(f"BytesInHeader: {info['bytes_in_header']}")
        print()
        print("Channel IDs in file order:")
        for idx, channel_id in enumerate(info["channel_ids"][: args.show_channels]):
            print(f"  file_index={idx:4d} channel_id={channel_id}")
        if len(info["channel_ids"]) > args.show_channels:
            print(f"  ... {len(info['channel_ids']) - args.show_channels} more")
        print()
        print("Data layout after the header:")
        print("  sample frame = one int16 per channel in the channel-id order above")
        print(f"  bytes per sample frame = {info['channel_count']} * 2 = {info['channel_count'] * 2}")
        return

    info = parse_nsx22_or_newer(path)
    print(f"file: {path}")
    print(f"FileTypeID: {info['file_type_id']}")
    print(f"FileSpec: {info['file_spec']}")
    print(f"Label: {info['label']}")
    print(f"Comment: {info['comment']}")
    print(f"TimeOrigin: {info['time_origin']}")
    print(f"ChannelCount: {info['channel_count']}")
    print(f"Period: {info['period']}")
    print(f"TimeStampResolution: {info['sample_resolution']}")
    print(f"SampleRateHz: {info['sample_rate_hz']:.6f}")
    print(f"BytesInHeader: {info['bytes_in_header']}")
    print(f"Expected extended header size per channel: {NSX22_EXT_SIZE} bytes")
    print()
    print("Extended headers in file order:")
    for idx, header in enumerate(info["extended_headers"][: args.show_channels]):
        print(
            "  "
            f"file_index={idx:4d} "
            f"electrode_id={header['electrode_id']:4d} "
            f"label={header['electrode_label']!r} "
            f"connector={header['physical_connector']} "
            f"pin={header['connector_pin']}"
        )
    if len(info["extended_headers"]) > args.show_channels:
        print(f"  ... {len(info['extended_headers']) - args.show_channels} more")

    print()
    print("Data layout after the header:")
    print(f"  first packet offset = {info['first_packet_offset']} bytes")
    print(f"  first packet header size = {info['first_packet_header_size']} bytes")
    print("  each sample frame stores one int16 per channel in the file order above")
    print(f"  bytes per sample frame = {info['channel_count']} * 2 = {info['channel_count'] * 2}")

    first_packet = info["first_packet"]
    if first_packet is not None:
        print()
        print("First packet header:")
        print(f"  header byte = {first_packet['header_byte']}")
        print(f"  timestamp = {first_packet['timestamp']}")
        print(f"  num_data_points = {first_packet['num_data_points']}")
        print(
            "  packet data bytes = "
            f"{first_packet['num_data_points']} * {info['channel_count']} * 2 = "
            f"{first_packet['num_data_points'] * info['channel_count'] * 2}"
        )


if __name__ == "__main__":
    main()
