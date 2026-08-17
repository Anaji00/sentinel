"use client";

import React from "react";
import ProvenanceBadge, { ProvenanceType } from "./ProvenanceBadge";

export interface ProvenanceInfo {
  source_type: ProvenanceType;
  computed_at?: string;
  methodology?: string;
  model_name?: string;
  model_confidence?: number;
  data_inputs?: string[];
  is_synthetic?: boolean;
}

interface ProvenanceValueProps {
  value: number | string | null | undefined;
  provenance?: ProvenanceInfo;
  format?: "currency" | "percent" | "number" | "raw";
  decimals?: number;
  prefix?: string;
  suffix?: string;
  showBadge?: boolean;
  className?: string;
}

export const ProvenanceValue: React.FC<ProvenanceValueProps> = ({
  value,
  provenance,
  format = "raw",
  decimals = 2,
  prefix = "",
  suffix = "",
  showBadge = true,
  className = "",
}) => {
  const isPlaceholder = !provenance || provenance.source_type === "disclosed_placeholder" || value === null || value === undefined;

  // Zero-Fabrication Rule: If placeholder or undefined, NEVER output a number
  if (isPlaceholder) {
    return (
      <div className={`inline-flex items-center gap-2 ${className}`}>
        <span className="font-mono text-slate-500 font-bold tracking-wider select-none" title="Pending telemetry/historical baseline">
          —
        </span>
        {showBadge && (
          <ProvenanceBadge
            sourceType="disclosed_placeholder"
            methodology={provenance?.methodology || "Insufficient historical data"}
          />
        )}
      </div>
    );
  }

  let formatted = String(value);
  const numVal = typeof value === "number" ? value : parseFloat(String(value));

  if (!isNaN(numVal)) {
    if (format === "currency") {
      if (Math.abs(numVal) >= 1e9) {
        formatted = `$${(numVal / 1e9).toFixed(decimals)}B`;
      } else if (Math.abs(numVal) >= 1e6) {
        formatted = `$${(numVal / 1e6).toFixed(decimals)}M`;
      } else {
        formatted = `$${numVal.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`;
      }
    } else if (format === "percent") {
      formatted = `${numVal.toFixed(decimals)}%`;
    } else if (format === "number") {
      formatted = numVal.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    }
  }

  const isLlm = provenance?.source_type === "llm_inference";
  const textStyle = isLlm ? "text-purple-300 font-medium" : "text-white font-bold";

  return (
    <div className={`inline-flex items-center gap-2 ${className}`}>
      <span className={`${textStyle} font-mono`}>
        {prefix}{formatted}{suffix}
      </span>
      {showBadge && provenance && (
        <ProvenanceBadge
          sourceType={provenance.source_type}
          methodology={provenance.methodology}
          modelName={provenance.model_name}
          confidence={provenance.model_confidence}
          dataInputs={provenance.data_inputs}
          isSynthetic={provenance.is_synthetic}
        />
      )}
    </div>
  );
};

export default ProvenanceValue;
