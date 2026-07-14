export const shellScreenshotOptions = {
  comparatorName: "pixelmatch",
  comparatorOptions: {
    allowedMismatchedPixelRatio: 0.02,
    includeAA: false,
    threshold: 0.2,
  },
  screenshotOptions: {
    animations: "allow",
    caret: "hide",
    scale: "css",
  },
} as const;
