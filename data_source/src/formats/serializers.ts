import type { DataPoint } from '../generator/datapoint';

export type DumpFormat = 'json' | 'ndjson' | 'csv';

export function isDumpFormat(x: string): x is DumpFormat {
  return x === 'json' || x === 'ndjson' || x === 'csv';
}

export function contentType(format: DumpFormat): string {
  switch (format) {
    case 'json':
      return 'application/json';
    case 'ndjson':
      return 'application/x-ndjson';
    case 'csv':
      return 'text/csv';
  }
}

const CSV_HEADER = 'ts,deviceId,metric,seriesId,value,unit\n';

function csvRow(p: DataPoint): string {
  // None of the fields contain commas/quotes, so plain join is safe here.
  return `${p.ts},${p.deviceId},${p.metric},${p.seriesId},${p.value},${p.unit}\n`;
}

/**
 * Incremental serializer. Feed it points one at a time; it returns the chunk of
 * text to write for each (and a final {@link end} chunk). Keeping the JSON array
 * bracketing/commas as state here lets the caller stream hundreds of thousands
 * of points without ever holding them all in memory.
 */
export class DumpSerializer {
  private started = false;

  constructor(private readonly format: DumpFormat) {}

  /** Text to emit before any points (CSV header / JSON opening bracket). */
  begin(): string {
    if (this.format === 'csv') return CSV_HEADER;
    if (this.format === 'json') return '[';
    return '';
  }

  row(p: DataPoint): string {
    switch (this.format) {
      case 'ndjson':
        return JSON.stringify(p) + '\n';
      case 'csv':
        return csvRow(p);
      case 'json': {
        const prefix = this.started ? ',' : '';
        this.started = true;
        return prefix + JSON.stringify(p);
      }
    }
  }

  end(): string {
    return this.format === 'json' ? ']' : '';
  }
}
