/**
 * Split leaked reasoning-tag blocks out of model text. Some open models served
 * through OpenAI-compatible gateways emit reasoning inline as tagged blocks
 * (`<think>` for DeepSeek/Qwen/Kimi, `<mm:think>` for MiniMax-M3) instead of a
 * separate reasoning field. MiniMax also emits a bare closing tag on
 * non-thinking turns, and a stream can end mid-thought with an unclosed
 * opener — text on the thinking side of either is treated as reasoning.
 */
export type ThinkSegment = { kind: "reasoning" | "text"; text: string };

const MARKER = /<(\/?)(?:think|thinking|mm:think)>/gi;

export function splitThinkTags(text: string): Array<ThinkSegment> {
  const segments: Array<ThinkSegment> = [];
  let mode: ThinkSegment["kind"] = "text";
  let last = 0;

  const push = (kind: ThinkSegment["kind"], end: number) => {
    if (end > last) segments.push({ kind, text: text.slice(last, end) });
  };

  for (const match of text.matchAll(MARKER)) {
    const isCloser = match[1] === "/";
    const index = match.index ?? 0;
    if (isCloser) {
      // A closer without an opener (MiniMax non-thinking turns) still means
      // everything before it was reasoning.
      push("reasoning", index);
      mode = "text";
      last = index + match[0].length;
    } else if (mode === "text") {
      push("text", index);
      mode = "reasoning";
      last = index + match[0].length;
    }
    // An opener while already inside reasoning is left as part of the block.
  }

  push(mode, text.length);
  return segments;
}
