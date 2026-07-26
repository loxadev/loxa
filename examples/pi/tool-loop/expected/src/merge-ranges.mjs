/**
 * Merge inclusive integer ranges that overlap or have no integer gap.
 * Return sorted ranges without mutating the caller's array.
 *
 * @param {Array<[number, number]>} ranges
 * @returns {Array<[number, number]>}
 */
export function mergeRanges(ranges) {
  if (!Array.isArray(ranges)) {
    throw new Error("ranges must be an array");
  }
  const sorted = ranges.map((range) => {
    if (!Array.isArray(range) || range.length !== 2) {
      throw new Error("each range must contain two integers");
    }
    const [start, end] = range;
    if (!Number.isInteger(start) || !Number.isInteger(end)) {
      throw new Error("range bounds must be integers");
    }
    if (start > end) {
      throw new Error("range start must not exceed its end");
    }
    return [start, end];
  }).sort((left, right) => left[0] - right[0] || left[1] - right[1]);

  const merged = [];
  for (const range of sorted) {
    const previous = merged.at(-1);
    if (previous && range[0] <= previous[1] + 1) {
      previous[1] = Math.max(previous[1], range[1]);
    } else {
      merged.push(range);
    }
  }
  return merged;
}
