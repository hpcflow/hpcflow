"""Keep each documentation section's sidebar independent of the other sections."""
from bs4 import BeautifulSoup
from furo.navigation import get_navigation_tree
from sphinx.errors import ExtensionError


def page_context(app, pagename, templatename, context, doctree):
    sections = context.get("sections", [])
    if not sections or "toctree" not in context:
        return

    active = next(
        ((target, label) for target, label in sections
         if pagename.startswith(target.rsplit("/", 1)[0] + "/")),
        None,
    )
    context["active_section"] = active
    if active is None:
        # Home, search and other pages outside a section offer a short site menu.
        context["section_navigation_tree"] = get_navigation_tree(context["toctree"](
            collapse=False, titles_only=True, maxdepth=1, includehidden=True,
        ))
        return

    target, label = active
    soup = BeautifulSoup(context["toctree"](
        collapse=False, titles_only=True, maxdepth=-1, includehidden=True,
    ), "html.parser")
    overview_url = context["pathto"](target)
    overview = soup.find("a", href=overview_url)
    if overview is None:
        raise ExtensionError(f"Navigation section {target!r} is missing from the toctree")

    children = overview.parent.find("ul", recursive=False)
    if children is not None:
        # Promote the selected section's children to the sidebar's top level.
        for item in children.find_all("li"):
            item["class"] = [
                f"toctree-l{int(css[9:]) - 1}" if css.startswith("toctree-l") else css
                for css in item.get("class", [])
            ]
    context["section_navigation_tree"] = get_navigation_tree(str(children) if children else "")
    context["section_overview_current"] = pagename == target


def setup(app):
    app.connect("html-page-context", page_context)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
