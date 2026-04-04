/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_S3_ENABLED: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
