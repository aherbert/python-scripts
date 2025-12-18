#!/usr/bin/env python3
"""Exports a plate."""

import argparse
import os

import numpy as np
from omero.cli import cli_login
from omero.gateway import BlitzGateway
from tifffile import imwrite


def main():
    parser = argparse.ArgumentParser(description="Exports a plate.")
    parser.add_argument(
        "plate_id",
        type=int,
        help="Plate ID",
    )
    _ = parser.add_argument(
        "--numpy",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Export as numpy",
    )
    args = parser.parse_args()

    with cli_login() as cli:
        conn = BlitzGateway(client_obj=cli._client)
        plate = conn.getObject("Plate", args.plate_id)
        if plate is None:
            raise Exception(f"Invalid plate: {args.plate_id}")

        dir = f"plate{args.plate_id}"
        os.makedirs(dir, exist_ok=True)

        wells = list(plate.listChildren())
        for well in wells:
            well_dir = os.path.join(dir, well.getWellPos())
            os.makedirs(well_dir, exist_ok=True)

            well_samples = list(well.listChildren())
            print(f"{well.getWellPos()}, # samples= {len(well_samples)}")

            for i, well_sample in enumerate(well_samples):
                image = well_sample.getImage()
                sizeZ = image.getSizeZ()
                sizeC = image.getSizeC()
                sizeT = image.getSizeT()
                print(f"Sample {i}: z={sizeZ}, c={sizeC}, t={sizeT}")

                pp = image.getPrimaryPixels()
                for z in range(sizeZ):
                    for c in range(sizeC):
                        for t in range(sizeT):
                            pixels = pp.getPlane(z, c, t)
                            if args.numpy:
                                np.save(
                                    os.path.join(well_dir, f"s{i}_z{z}_c{c}_t{t}.npy"),
                                    pixels,
                                )
                            else:
                                imwrite(
                                    os.path.join(well_dir, f"s{i}_z{z}_c{c}_t{t}.tiff"),
                                    pixels,
                                )


if __name__ == "__main__":
    main()
