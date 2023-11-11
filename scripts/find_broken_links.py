from pathlib import Path

from rich.progress import track

src = "/mnt/Toshiba/.Grabber/"

gen = Path(src).rglob("*")
gen_ = (x for x in track(gen) if not ((resolved := x.resolve()) == x or resolved.exists()))

lst = []
for p in gen_:
    print(p)
    lst.append(p)
response = input("These links lead to empty files. Delete them? y/N:")
if response.lower().startswith("y"):
    print("Deleting...")
    for i in lst:
        i.unlink()
