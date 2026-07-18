import { describe, expect, it } from "vitest"

import { splitThinkTags } from "./thinkTags"

describe("splitThinkTags", () => {
  it("passes through text without tags", () => {
    expect(splitThinkTags("Opened PR #8, all set.")).toEqual([
      { kind: "text", text: "Opened PR #8, all set." },
    ])
  })

  it("splits a paired mm:think block into reasoning + text", () => {
    expect(splitThinkTags("<mm:think>plan things</mm:think>Done.")).toEqual([
      { kind: "reasoning", text: "plan things" },
      { kind: "text", text: "Done." },
    ])
  })

  it("handles plain think tags and multiple blocks in order", () => {
    expect(splitThinkTags("<think>a</think>one<think>b</think>two")).toEqual([
      { kind: "reasoning", text: "a" },
      { kind: "text", text: "one" },
      { kind: "reasoning", text: "b" },
      { kind: "text", text: "two" },
    ])
  })

  it("treats text before a bare closer as reasoning", () => {
    expect(splitThinkTags("leaked reasoning</mm:think>Visible reply")).toEqual([
      { kind: "reasoning", text: "leaked reasoning" },
      { kind: "text", text: "Visible reply" },
    ])
  })

  it("treats an unclosed opener's tail as reasoning", () => {
    expect(splitThinkTags("Hello<mm:think>stream died mid-thought")).toEqual([
      { kind: "text", text: "Hello" },
      { kind: "reasoning", text: "stream died mid-thought" },
    ])
  })

  it("returns only reasoning for a thinking-only message", () => {
    expect(splitThinkTags("<mm:think>let me try heredoc</mm:think>")).toEqual([
      { kind: "reasoning", text: "let me try heredoc" },
    ])
  })
})
