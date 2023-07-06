import argparse
from pathlib import Path

import polars as pl

from dataset_filters import DatasetBuilder, HashFilter
from tqdm import tqdm


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="folder to scan")
    parser.add_argument("category", help="the category from the database to sort by")
    return parser


if __name__ == "__main__":
    args = get_parser().parse_args()

    folder = Path(args.input)
    new_folder = folder.parent / "linked"
    new_folder.mkdir(exist_ok=True)

    exts = [".jpg", ".jpeg", ".png", ".webp"]
    filelist = list(filter(lambda i: i.suffix in exts, folder.rglob("*")))
    db = DatasetBuilder("filedb.feather")
    assert args.category in db.df.columns, f"selected category is not in {db.df.columns}"
    db.add_filters(HashFilter())
    db.populate_df(filelist)
    file_data = db.df.filter(pl.col("path").is_in(list(map(str, filelist))))
    with tqdm(file_data.iter_rows(named=True), total=len(file_data)) as t:
        for data in t:
            pth = Path(data["path"])
            hash_ = str(data[args.category])
            new_path: Path = (new_folder / f"{hash_}_{pth.stem}").with_suffix(pth.suffix)
            if not new_path.exists():
                new_path.symlink_to(pth)
                t.set_description_str(hash_)
