import re

LOGIN = re.compile(r"(?<![\w.])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))(?![\w/.-]*\.\w)")
YAML_KEY = re.compile(r"^(approvers|reviewers|maintainers)\s*:\s*$", re.I)
YAML_ITEM = re.compile(r"^\s*-\s*([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))\s*(#.*)?$")


def owner_logins(text: str) -> set[str]:
    # "@alice" counts, "@org/team" and e-mail addresses do not; Kubernetes OWNERS files list plain names.
    text = text or ""
    found = {m.group(1).lower() for m in LOGIN.finditer(text) if text[m.end():m.end() + 1] != "/"}
    in_list = False
    for line in text.splitlines():
        if YAML_KEY.match(line.strip()):
            in_list = True
            continue
        item = YAML_ITEM.match(line) if in_list else None
        if item:
            found.add(item.group(1).lower())
        elif line.strip() and not line.startswith((" ", "\t", "-")):
            in_list = False
    return found
