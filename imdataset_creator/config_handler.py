from collections.abc import Generator, Iterable
from pathlib import Path
from typing import overload

from .alphanumeric_sort import alphanumeric_sort
from .configs import MainConfig, _repr_indent
from .datarules import Input, Output, Producer, Rule
from .datarules.base_rules import PathGenerator
from .file import File
from .scenarios import FileScenario, OutputScenario


class ConfigHandler:
    def __init__(self, cfg: MainConfig):
        # generate `Input`s
        self.inputs: list[Input] = [
            Input.from_cfg(folder["data"]) for folder in cfg["inputs"]
        ]
        # generate `Output`s
        self.outputs: list[Output] = [
            Output.from_cfg(folder["data"]) for folder in cfg["output"]
        ]
        # generate `Producer`s
        self.producers: list[Producer] = [
            Producer.all_producers[p["name"]].from_cfg(p["data"])
            for p in cfg["producers"]
        ]

        # generate `Rule`s
        self.rules: list[Rule] = [
            Rule.all_rules[r["name"]].from_cfg(r["data"]) for r in cfg["rules"]
        ]

    @overload
    def gather_images(
        self, sort=True, reverse=False
    ) -> Generator[tuple[Path, list[Path]], None, None]:
        ...

    @overload
    def gather_images(
        self, sort=False, reverse=False
    ) -> Generator[tuple[Path, PathGenerator], None, None]:
        ...

    def gather_images(
        self, sort=False, reverse=False
    ) -> Generator[tuple[Path, PathGenerator | list[Path]], None, None]:
        for input_ in self.inputs:
            gen = input_.run()
            if sort:
                yield input_.folder, list(
                    map(
                        Path,
                        sorted(map(str, gen), key=alphanumeric_sort, reverse=reverse),
                    )
                )
            else:
                yield input_.folder, gen

    def get_outputs(self, file: File) -> list[OutputScenario]:
        return [
            OutputScenario(str(pth), output.filters)
            for output in self.outputs
            if not (pth := output.folder / Path(output.format_file(file))).exists()
            or output.overwrite
        ]

    def parse_files(self, files: Iterable[File]) -> Generator[FileScenario, None, None]:
        for file in files:
            if out_s := self.get_outputs(file):
                yield FileScenario(file, out_s)

    def __repr__(self):
        i = ",\n".join(map(_repr_indent, map(repr, self.inputs)))
        o = ",\n".join(map(_repr_indent, map(repr, self.outputs)))
        p = ",\n".join(map(_repr_indent, map(repr, self.producers)))
        r = ",\n".join(map(_repr_indent, map(repr, self.rules)))
        attrs = ",\n".join(
            [
                _repr_indent(f"inputs=[\n{i}\n]"),
                _repr_indent(f"outputs=[\n{o}\n]"),
                _repr_indent(f"producers=[\n{p}\n]"),
                _repr_indent(f"rules=[\n{r}\n]"),
            ]
        )
        return "\n".join([f"{self.__class__.__name__}(", attrs, ")"])
