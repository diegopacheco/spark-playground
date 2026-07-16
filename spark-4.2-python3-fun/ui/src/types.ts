export type Runtime = {
  sparkVersion: string;
  javaVersion: string;
  builtinFunctions: number;
  arrowPythonUdfDefault: string;
  scalaCompiler?: string;
  scalaRuntimeStdlib?: string;
  pythonVersion?: string;
  pandasVersion?: string;
  pyarrowVersion?: string;
};

export type RegionRow = { region: string; revenue: number; customers: number };

export type MetricView = {
  yaml: string;
  byRegion: RegionRow[];
  governedTotalCustomers: number;
  naiveSummedCustomers: number;
};

export type NearestRow = { name: string; distance: number; similarity: number };
export type NormRow = { name: string; norm: number; unit: number[] };

export type VectorSection = {
  queryText: string;
  queryVector: number[];
  nearest: NearestRow[];
  norms: NormRow[];
};

export type TopKRow = { item: string; count: number };

export type Sketches = {
  topK: TopKRow[];
  exactDistinct: number;
  approxDistinct: number;
  approxMethod: string;
};

export type GeoSample = {
  site: string;
  srid: number;
  geometryType: string;
  geographyType: string;
};

export type Geo = { sample: GeoSample[]; availableStFunctions: string[] };

export type ArrowUdf = {
  rows: number;
  arrowEnabledByDefault: string;
  sample: { name: string; boosted: number }[];
};

export type DataSource = { rows: { id: number; label: string }[] };

export type Results = {
  runtime: Runtime;
  metricView: MetricView;
  vector: VectorSection;
  sketches: Sketches;
  geo: Geo;
  arrowUdf?: ArrowUdf;
  dataSource?: DataSource;
};
