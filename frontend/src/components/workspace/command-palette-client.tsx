"use client";

import dynamic from "next/dynamic";

const CommandPaletteDynamic = dynamic(
  () => import("@/components/workspace/command-palette").then((m) => m.CommandPalette),
  { ssr: false },
);

export function CommandPaletteClient() {
  return <CommandPaletteDynamic />;
}
