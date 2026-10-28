"""Check local HTML links, fragment targets and referenced assets in a built preview."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Links(HTMLParser):
    def __init__(self, path):
        super().__init__()
        self.ids = set()
        self.links = []
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for key in ("href", "src"):
            if attrs.get(key):
                self.links.append(attrs[key])


def check(root):
    root = root.resolve()
    documents = {path.resolve(): Links(path) for path in root.rglob("*.html")}
    errors = []
    for path, document in documents.items():
        for link in document.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            target = (path.parent / unquote(url.path)).resolve() if url.path else path
            if target.is_dir():
                target /= "index.html"
            if not target.is_file():
                errors.append(f"{path.relative_to(root)}: missing {link}")
            elif url.fragment and target in documents:
                if unquote(url.fragment) not in documents[target].ids:
                    errors.append(f"{path.relative_to(root)}: missing fragment {link}")
    if errors:
        raise ValueError("\n".join(errors))
    print(f"Checked local links, fragments and assets in {len(documents)} HTML files.")


if __name__ == "__main__":
    check(Path(__file__).resolve().parent / "_build")
