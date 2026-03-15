from typing import List, Tuple

from .helpers import escape_text, escape_with_breaks
from .runtime import render_html_to_image


def render_report_image(
    title: str,
    sections: List[Tuple[str, List[str]]],
    subtitle: str = "",
    footer: str = "",
    width: int = 1080,
) -> bytes:
    section_html: List[str] = []
    for sec_title, sec_lines in sections:
        lis = "".join([f"<li>{escape_with_breaks(line)}</li>" for line in sec_lines])
        section_html.append(
            f"""
<section class=\"section\">
  <h2 class=\"section-title\">{escape_text(sec_title)}</h2>
  <ul class=\"section-body\">{lis}</ul>
</section>
"""
        )

    subtitle_html = f"<p>{escape_with_breaks(subtitle)}</p>" if subtitle else ""
    footer_html = f"<div class=\"footer\">{escape_with_breaks(footer)}</div>" if footer else ""

    body = f"""
<div class=\"card\">
  <div class=\"head\">
    <h1>{escape_text(title)}</h1>
    {subtitle_html}
  </div>
  <div class=\"content\">
    {''.join(section_html)}
    {footer_html}
  </div>
</div>
"""
    return render_html_to_image(body, width=width)
