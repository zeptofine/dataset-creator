from pathlib import Path
from tqdm import tqdm

src = "/mnt/Toshiba/.Grabber/"

gen = Path(src).rglob("*/**/*")

lst: list[Path] = []
for path in tqdm(gen):
    x = path.resolve()
    if x != path and not x.exists():
        lst.append(path)
        print(path)

response = input("These links lead to empty files. Delete them? y/N:")
if response.lower().startswith("y"):
    print("Deleting...")
    [i.unlink() for i in lst]
