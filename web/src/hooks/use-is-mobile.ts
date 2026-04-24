"use client";

import { useEffect, useState } from "react";

interface UADataLike {
  mobile?: boolean;
}

function detectMobile(): boolean {
  if (typeof navigator === "undefined") return false;
  const uaData = (navigator as Navigator & { userAgentData?: UADataLike }).userAgentData;
  if (uaData && typeof uaData.mobile === "boolean") return uaData.mobile;
  return /Mobi/i.test(navigator.userAgent);
}

export function useIsMobile(): boolean {
  const [isMobile, setIsMobile] = useState(false);
  useEffect(() => {
    setIsMobile(detectMobile());
  }, []);
  return isMobile;
}
