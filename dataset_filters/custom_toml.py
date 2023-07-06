import toml


class TomlCustomCommentEncoder(
    toml.TomlEncoder
):  # I'm making this because I have absolutely no idea how toml.TomlPreserveCommentEncoder works
    """A toml encoder that changes keys that start with !# to create a comment for another key."""

    def dump_sections(self, o, sup: str):
        if isinstance(o, dict):
            o, orig = o.copy(), o
            comments = {}
            for name, comment in orig.items():
                if isinstance(name, str) and name.startswith("!#"):
                    assert isinstance(comment, str)
                    comments[name[2:]] = comment
                    o.pop(name)

            out = super().dump_sections(o, sup)

            if comments:  # reinsert the comments on top of the dump
                new = []
                for line in out[0].splitlines():
                    if (name := line.split(" ", 1)[0]) in comments:
                        new.append(f"{line} #{comments[name]}")
                    else:
                        new.append(line)
                return ("\n".join(new), out[1])
        return super().dump_sections(o, sup)

    pass


class TomlCustomCommentDecoder(toml.TomlPreserveCommentDecoder):
    def preserve_comment(self, line_no, key, comment, beginline):
        self.saved_comments[line_no] = (key, comment)  # type: ignore

    def embed_comments(self, idx, currentlevel) -> None:
        if idx not in self.saved_comments:
            return

        key, comment = self.saved_comments[idx]  # type: ignore
        if key:
            if comment.strip().startswith("#"):
                comment = comment.strip()[1:]
            currentlevel[f"!#{key}"] = comment

    pass
