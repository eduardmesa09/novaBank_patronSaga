type IconProps = { className?: string };

const base = {
  width: 24,
  height: 24,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.6,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
};

/** Marca de NovaBank: rombo con una "N" implícita en la diagonal. */
export function BrandMark({ className }: IconProps) {
  return (
    <svg className={className} width="34" height="34" viewBox="0 0 34 34" aria-hidden>
      <rect x="4" y="4" width="26" height="26" rx="3" transform="rotate(45 17 17)" fill="#0b5cab" />
      <path d="M12 21V13l10 8v-8" stroke="#fff" strokeWidth="2.2" fill="none" strokeLinecap="round" />
    </svg>
  );
}

export function ChevronRight({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="m9 6 6 6-6 6" />
    </svg>
  );
}

export function CheckCircle({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <circle cx="12" cy="12" r="9" />
      <path d="m8.5 12 2.5 2.5 4.5-5" />
    </svg>
  );
}

export function Wallet({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="M3 7.5A2.5 2.5 0 0 1 5.5 5H18a2 2 0 0 1 2 2v1" />
      <path d="M3 7.5V17a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-2" />
      <path d="M21 10h-4a2 2 0 0 0 0 4h4z" />
    </svg>
  );
}

export function ShieldAlert({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="M12 3 5 6v5.5c0 4.2 2.9 7.6 7 9.5 4.1-1.9 7-5.3 7-9.5V6z" />
      <path d="M12 9v3.5" />
      <circle cx="12" cy="16" r="0.6" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function NetworkDown({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="M2.5 9a15 15 0 0 1 8-3.8" />
      <path d="M13.5 5.2A15 15 0 0 1 21.5 9" />
      <path d="M6 12.5a10 10 0 0 1 4-2.2" />
      <path d="M14 10.3a10 10 0 0 1 4 2.2" />
      <path d="M9.5 16a5 5 0 0 1 5 0" />
      <circle cx="12" cy="19.5" r="0.7" fill="currentColor" stroke="none" />
      <path d="m3 3 18 18" strokeWidth="1.8" />
    </svg>
  );
}

export function Repeat({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="M4 10V8a3 3 0 0 1 3-3h10l-2.5-2.5M20 14v2a3 3 0 0 1-3 3H7l2.5 2.5" />
    </svg>
  );
}

export function Undo({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="M4 9h11a4.5 4.5 0 0 1 0 9h-5" />
      <path d="m8 5-4 4 4 4" />
    </svg>
  );
}

export function Clock({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7.5V12l3 1.8" />
    </svg>
  );
}

export function XCircle({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <circle cx="12" cy="12" r="9" />
      <path d="m9.5 9.5 5 5m0-5-5 5" />
    </svg>
  );
}

export function MinusCircle({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <circle cx="12" cy="12" r="9" />
      <path d="M8.5 12h7" />
    </svg>
  );
}

export function Bank({ className }: IconProps) {
  return (
    <svg {...base} className={className} aria-hidden>
      <path d="m3 9 9-5 9 5" />
      <path d="M5 9v9m4.5-9v9m5-9v9M19 9v9" />
      <path d="M3 21h18" />
    </svg>
  );
}
