"""Strip leaked reasoning-tag blocks from model text.

Some open models served through OpenAI-compatible gateways leak reasoning into
visible text as tagged blocks (`<think>` for DeepSeek/Qwen/Kimi, `<mm:think>`
for MiniMax-M3) instead of a separate reasoning field. MiniMax also emits a
bare closing tag on non-thinking turns and can end a stream mid-thought with
an unclosed opener.

Only use the stripped text to decide whether a message has user-visible
content — never write it back into the message, since MiniMax requires
thinking blocks to be passed back verbatim in conversation history.
"""

import re

_TAG = r"(?:think|thinking|mm:think)"
_PAIRED = re.compile(rf"<({_TAG})>.*?</\1>", re.DOTALL | re.IGNORECASE)
_BARE_CLOSER = re.compile(rf"\A.*</{_TAG}>", re.DOTALL | re.IGNORECASE)
_UNCLOSED_OPENER = re.compile(rf"<{_TAG}>.*\Z", re.DOTALL | re.IGNORECASE)


def strip_think_tags(text: str) -> str:
    text = _PAIRED.sub("", text)
    text = _BARE_CLOSER.sub("", text)
    text = _UNCLOSED_OPENER.sub("", text)
    return text
