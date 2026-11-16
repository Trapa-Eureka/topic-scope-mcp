const numbers = new Intl.NumberFormat("en-US");

export function count(n: number): string {
  return numbers.format(n);
}

/** A signed change: "+12", "−3", "0". The minus is a real minus sign, which lines up with the plus. */
export function change(n: number): string {
  if (n > 0) return `+${numbers.format(n)}`;
  if (n < 0) return `−${numbers.format(-n)}`;
  return "0";
}

/** An age in words a glance can take in: "4 s", "3 min", "2 h 5 min". Empty for "no age". */
export function age(seconds: number): string {
  if (seconds < 0) return "";
  if (seconds < 90) return `${seconds} s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 90) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest === 0 ? `${hours} h` : `${hours} h ${rest} min`;
}

/** "14:05:43" from an ISO time, in UTC, as the server reports it. */
export function clock(iso: string): string {
  if (!iso) return "";
  const t = iso.indexOf("T");
  return t < 0 ? iso : iso.slice(t + 1, t + 9);
}

export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} kB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}
