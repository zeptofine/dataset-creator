import argparse
from pathlib import Path

import imagehash
import polars as pl

from dataset_filters import DatasetBuilder, HashFilter
from tqdm import tqdm


IMHASH_TYPES = {
    "average": imagehash.average_hash,
    "crop_resistant": imagehash.crop_resistant_hash,
    "color": imagehash.colorhash,
    "dhash": imagehash.dhash,
    "dhash_vertical": imagehash.dhash_vertical,
    "phash": imagehash.phash,
    "phash_simple": imagehash.phash_simple,
    "whash": imagehash.whash,
}


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="folder to scan", required=True)
    parser.add_argument("type", choices=IMHASH_TYPES.keys(), default="average")
    return parser


if __name__ == "__main__":
    args = get_parser().parse_args()
    hasher = IMHASH_TYPES[args.type]

    folder = Path(args.input)
    new_folder = folder.parent / "linked"
    new_folder.mkdir(exist_ok=True)

    exts = [".jpg", ".jpeg", ".png", ".webp"]
    filelist = list(filter(lambda i: i.suffix in exts, folder.rglob("*")))
    db = DatasetBuilder("filedb.feather")
    db.add_filters(HashFilter(args.type))
    db.populate_df(filelist)
    file_data = db.df.filter(pl.col("path").is_in(list(map(str, filelist))))
    with tqdm(file_data.sort("hash").iter_rows(named=True), total=len(file_data)) as t:
        for data in t:
            pth = Path(data["path"])
            hash_ = data["hash"]
            new_path: Path = new_folder / hash_
            new_path = new_path.with_name(f"{new_path.stem}_{pth.stem}").with_suffix(pth.suffix)
            if not new_path.exists():
                new_path.symlink_to(pth)
                t.set_description_str(hash_)
