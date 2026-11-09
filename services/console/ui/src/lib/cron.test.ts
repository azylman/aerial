import { describe, expect, it } from 'vitest';
import { formatCronExpression, ordinal } from './cron';

describe('ordinal', () => {
  it.each([
    { n: 1, want: '1st' },
    { n: 2, want: '2nd' },
    { n: 3, want: '3rd' },
    { n: 4, want: '4th' },
    { n: 11, want: '11th' },
    { n: 12, want: '12th' },
    { n: 13, want: '13th' },
    { n: 21, want: '21st' },
    { n: 22, want: '22nd' },
    { n: 23, want: '23rd' },
    { n: 30, want: '30th' },
    { n: 111, want: '111th' },
  ])('$n -> $want', ({ n, want }) => {
    expect(ordinal(n)).toBe(want);
  });
});

describe('formatCronExpression', () => {
  it.each([
    { expr: null, want: '' },
    { expr: undefined, want: '' },
    { expr: '', want: '' },
    { expr: '   ', want: '' },
    { expr: '@yearly', want: 'Every year on Jan 1st at 00:00' },
    { expr: '@ANNUALLY', want: 'Every year on Jan 1st at 00:00' },
    { expr: '@monthly', want: '1st of every month at 00:00' },
    { expr: '@weekly', want: 'Every week on Sunday at 00:00' },
    { expr: '@daily', want: 'Every day at 00:00' },
    { expr: '@midnight', want: 'Every day at 00:00' },
    { expr: ' @hourly ', want: 'Every hour' },
    { expr: '@reboot', want: '@reboot' },
    { expr: '0 9 * *', want: '0 9 * *' },
    { expr: '* * * * *', want: 'Every minute' },
    { expr: '*/15 * * * *', want: 'Every 15 minutes' },
    { expr: '0 */6 * * *', want: 'Every 6 hours' },
    { expr: '5 */6 * * *', want: '5 */6 * * *' },
    { expr: '5 * * * *', want: '5 * * * *' },
    { expr: '*/5 9 * * *', want: '*/5 9 * * *' },
    { expr: '0,30 9 * * *', want: '0,30 9 * * *' },
    { expr: '0  9   *  *  *', want: 'Every day at 09:00' },
    { expr: '30 7 * * *', want: 'Every day at 07:30' },
    { expr: '0 9 * * 1-5', want: 'Weekdays (Mon–Fri) at 09:00' },
    { expr: '0 9 * * mon-fri', want: 'Weekdays (Mon–Fri) at 09:00' },
    { expr: '0 10 * * 0,6', want: 'Weekends (Sat–Sun) at 10:00' },
    { expr: '0 10 * * 6,0', want: 'Weekends (Sat–Sun) at 10:00' },
    { expr: '0 10 * * SAT,SUN', want: 'Weekends (Sat–Sun) at 10:00' },
    { expr: '0 10 * * sun,sat', want: 'Weekends (Sat–Sun) at 10:00' },
    { expr: '0 8 * * 0', want: 'Every Sunday at 08:00' },
    { expr: '0 8 * * 3', want: 'Every Wednesday at 08:00' },
    { expr: '0 8 * * 7', want: 'Every Sunday at 08:00' },
    { expr: '0 9 * * 1,3,5', want: 'Mon, Wed, Fri at 09:00' },
    { expr: '0 9 * * 1-3', want: '1-3 at 09:00' },
    { expr: '0 9 * * 8', want: '8 at 09:00' },
    { expr: '0 9 * * MON,9', want: 'MON, 9 at 09:00' },
    { expr: '0 12 1 * *', want: '1st of every month at 12:00' },
    { expr: '0 12 22 * *', want: '22nd of every month at 12:00' },
    { expr: '0 12 32 * *', want: 'At 12:00 (0 12 32 * *)' },
    { expr: '0 12 1-15 * *', want: 'At 12:00 (0 12 1-15 * *)' },
    { expr: '0 0 1 1 *', want: 'Every year on Jan 1st at 00:00' },
    { expr: '15 18 3 12 *', want: 'Every year on Dec 3rd at 18:15' },
    { expr: '0 0 1 13 *', want: 'At 00:00 (0 0 1 13 *)' },
    { expr: '0 0 L 2 *', want: 'At 00:00 (0 0 L 2 *)' },
    { expr: '0 0 * 2 *', want: 'At 00:00 (0 0 * 2 *)' },
    { expr: '0 0 1 * 1', want: 'At 00:00 (0 0 1 * 1)' },
  ])('$expr -> $want', ({ expr, want }) => {
    expect(formatCronExpression(expr)).toBe(want);
  });
});
