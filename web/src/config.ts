export interface SwarunConfig {
  s3Enabled: boolean;
}

declare global {
  interface Window {
    SWARUN_CONFIG?: SwarunConfig;
  }
}

export const getConfig = (): SwarunConfig => {
  // window.SWARUN_CONFIG (Go backend injection) takes precedence.
  // Fallback to VITE_S3_ENABLED (env var for dev environment).
  if (window.SWARUN_CONFIG) {
    return window.SWARUN_CONFIG;
  }

  return {
    s3Enabled: import.meta.env.VITE_S3_ENABLED === "true",
  };
};
