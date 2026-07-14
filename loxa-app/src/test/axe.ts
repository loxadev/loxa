import axe from "axe-core";
import { expect } from "vitest";

export async function expectNoAxeViolations(root: Element | Document = document, options: axe.RunOptions = {}) {
  const result = await axe.run(root, options);
  expect(result.violations.map(({ id, nodes }) => ({ id, targets: nodes.map(({ target }) => target) }))).toEqual([]);
}
