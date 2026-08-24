export type Product = {
  id: number;
  name: string;
  description: string;
  price: string;
  tags: string[];
  updated_at: string;
};

export type Review = {
  id: number;
  product_id: number;
  author: string;
  rating: number;
  comment: string;
};

export type Propagation = { landed: boolean; ms: number };

// One entry per completed write; the header renders the recent ones with
// their measured times so a session becomes a visible record of runs.
export type HistoryEntry = {
  label: string;
  propagation: Propagation;
  extra?: string;
  at: number;
};

export type SearchHit = {
  id: string;
  source: Record<string, unknown>;
  highlight?: Record<string, string[]>;
};

// indexExists is null until the first search returns; the UI shows neither
// documents nor the "index missing" notice while it is null.
export type ClientSearchResult = {
  indexExists: boolean | null;
  total: number;
  hits: SearchHit[];
};
