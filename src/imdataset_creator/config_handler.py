from collections.abc import Generator, Iterable
from pathlib import Path
from typing import overload

from .alphanumeric_sort import alphanumeric_sort
from .configs import MainConfig
from .datarules import Input, Output, Producer, Rule
from .datarules.base_rules import PathGenerator
from .file import File
from .scenarios import FileScenario, OutputScenario


class ConfigHandler:
    def __init__(self, cfg: MainConfig):
        # generate `Input`s
        self.inputs = [Input.from_cfg(folder["data"]) for folder in cfg["inputs"]]
        # generate `Producer`s
        self.producers = [Producer.all_producers[p["name"]].from_cfg(p["data"]) for p in cfg["producers"]]

        # generate `Rule`s
        self.rules = [Rule.all_rules[r["name"]].from_cfg(r["data"]) for r in cfg["rules"]]

        # generate test kwargs
        tkwargs = {}
        for producer in self.producers:
            for name, production in producer.produces.items():
                tkwargs[name] = production.template

        # generate `Output`s
        self.outputs = [Output.from_cfg(folder["data"], tkwargs) for folder in cfg["output"]]

    @overload
    def gather_images(self, sort=True, reverse=False) -> Generator[tuple[Path, list[Path]], None, None]:
        ...

    @overload
    def gather_images(self, sort=False, reverse=False) -> Generator[tuple[Path, PathGenerator], None, None]:
        ...

    def gather_images(self, sort=False, reverse=False):
        for input_ in self.inputs:
            gen = input_.run()
            if sort:
                yield (
                    input_.folder,
                    list(map(Path, sorted(map(str, gen), key=alphanumeric_sort, reverse=reverse))),
                )
            else:
                yield input_.folder, gen

    def get_outputs(self, file: File) -> list[OutputScenario]:
        return [
            OutputScenario(
                str(pth),
                output.filters,
            )
            for output in self.outputs
            if (pth := output.check_validity(file))
        ]

    def parse_files(self, files: Iterable[File]) -> Generator[FileScenario, None, None]:
        return (FileScenario(file, out_s) for file in files if (out_s := self.get_outputs(file)))

    def __repr__(self) -> str:
        attrlist = [f"{key}={val!r}" for key, val in vars(self).items() if all(k not in key for k in ("__",))]
        return f"{self.__class__.__name__}({', '.join(attrlist)})"
