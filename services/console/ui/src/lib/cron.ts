// Human-readable descriptions for 5-field cron expressions and @macros, ported from
// formatCronExpression in static/app.js. Numeric fields are parsed strictly (whole field must be
// digits) so lists and ranges such as `1,3,5` or `1-15` are never misread as their first number.

const MACROS: Readonly<Record<string, string>> = Object.freeze({
  '@yearly': 'Every year on Jan 1st at 00:00',
  '@annually': 'Every year on Jan 1st at 00:00',
  '@monthly': '1st of every month at 00:00',
  '@weekly': 'Every week on Sunday at 00:00',
  '@daily': 'Every day at 00:00',
  '@midnight': 'Every day at 00:00',
  '@hourly': 'Every hour',
});

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'] as const;
// Cron allows both 0 and 7 for Sunday.
const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'] as const;
const DAYS_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as const;

const WEEKDAYS = new Set(['1-5', 'MON-FRI']);
const WEEKENDS = new Set(['0,6', '6,0', 'SAT,SUN', 'SUN,SAT']);

/** English ordinal: 1st, 2nd, 3rd, 4th, 11th–13th, 21st, … */
export function ordinal(n: number): string {
  const mod100 = n % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${n}th`;
  switch (n % 10) {
    case 1:
      return `${n}st`;
    case 2:
      return `${n}nd`;
    case 3:
      return `${n}rd`;
    default:
      return `${n}th`;
  }
}

/** Integer value of an all-digit cron field, or NaN for `*`, lists, ranges and steps. */
function strictInt(field: string): number {
  return /^\d+$/.test(field) ? Number(field) : Number.NaN;
}

const inRange = (n: number, lo: number, hi: number): boolean => n >= lo && n <= hi;

function describeDayOfWeek(dow: string, time: string): string {
  const upper = dow.toUpperCase();
  if (WEEKDAYS.has(upper)) return `Weekdays (Mon–Fri) at ${time}`;
  if (WEEKENDS.has(upper)) return `Weekends (Sat–Sun) at ${time}`;
  const day = strictInt(dow);
  if (inRange(day, 0, 7)) return `Every ${DAYS[day]} at ${time}`;
  const names = dow.split(',').map((part) => {
    const trimmed = part.trim();
    const n = strictInt(trimmed);
    return inRange(n, 0, 7) ? DAYS_SHORT[n] : trimmed;
  });
  return `${names.join(', ')} at ${time}`;
}

/** Describes a cron expression; unrecognised shapes are returned verbatim (or annotated). */
export function formatCronExpression(cronExpr: string | null | undefined): string {
  const expr = (cronExpr ?? '').trim();
  if (!expr) return '';

  const macro = MACROS[expr.toLowerCase()];
  if (macro !== undefined) return macro;

  const fields = expr.split(/\s+/);
  if (fields.length !== 5) return expr;
  const [minStr, hourStr, domStr, monStr, dowStr] = fields as [string, string, string, string, string];
  const restWild = domStr === '*' && monStr === '*' && dowStr === '*';

  if (restWild && hourStr === '*') {
    if (minStr === '*') return 'Every minute';
    if (minStr.startsWith('*/')) return `Every ${minStr.slice(2)} minutes`;
  }
  if (restWild && minStr === '0' && hourStr.startsWith('*/')) return `Every ${hourStr.slice(2)} hours`;

  const m = strictInt(minStr);
  const h = strictInt(hourStr);
  if (Number.isNaN(m) || Number.isNaN(h)) return expr;
  const time = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;

  if (restWild) return `Every day at ${time}`;
  if (domStr === '*' && monStr === '*') return describeDayOfWeek(dowStr, time);

  if (dowStr === '*') {
    const dom = strictInt(domStr);
    if (monStr === '*') {
      if (inRange(dom, 1, 31)) return `${ordinal(dom)} of every month at ${time}`;
    } else {
      const mon = strictInt(monStr);
      if (!Number.isNaN(dom) && inRange(mon, 1, 12)) return `Every year on ${MONTHS[mon - 1]} ${ordinal(dom)} at ${time}`;
    }
  }
  return `At ${time} (${expr})`;
}
