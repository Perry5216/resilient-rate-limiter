// Shared clock seam (ADR 0002). Default clock: process.hrtime.bigint() converted
// to fractional milliseconds — microsecond precision, monotonic.
export function hrtimeMs() {
  return Number(process.hrtime.bigint()) / 1000;
}
