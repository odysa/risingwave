/* eslint-disable */
import { ExprNode } from "./expr";
import {
  ColumnCatalog,
  ColumnOrder,
  Field,
  RowFormatType,
  rowFormatTypeFromJSON,
  rowFormatTypeToJSON,
} from "./plan_common";

export const protobufPackage = "catalog";

/**
 * The rust prost library always treats uint64 as required and message as
 * optional. In order to allow `row_id_index` as optional field in
 * `StreamSourceInfo` and `TableSourceInfo`, we wrap uint64 inside this message.
 */
export interface ColumnIndex {
  index: number;
}

export interface StreamSourceInfo {
  rowFormat: RowFormatType;
  rowSchemaLocation: string;
  useSchemaRegistry: boolean;
  protoMessageName: string;
  csvDelimiter: number;
  csvHasHeader: boolean;
}

export interface Source {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  /**
   * The column index of row ID. If the primary key is specified by the user,
   * this will be `None`.
   */
  rowIdIndex:
    | ColumnIndex
    | undefined;
  /** Columns of the source. */
  columns: ColumnCatalog[];
  /**
   * Column id of the primary key specified by the user. If the user does not
   * specify a primary key, the vector will be empty.
   */
  pkColumnIds: number[];
  /** Properties specified by the user in WITH clause. */
  properties: { [key: string]: string };
  owner: number;
  info: StreamSourceInfo | undefined;
}

export interface Source_PropertiesEntry {
  key: string;
  value: string;
}

export interface Sink {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  columns: ColumnCatalog[];
  pk: ColumnOrder[];
  dependentRelations: number[];
  distributionKey: number[];
  /** pk_indices of the corresponding materialize operator's output. */
  streamKey: number[];
  appendOnly: boolean;
  owner: number;
  properties: { [key: string]: string };
  definition: string;
}

export interface Sink_PropertiesEntry {
  key: string;
  value: string;
}

export interface Index {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  owner: number;
  indexTableId: number;
  primaryTableId: number;
  /**
   * Only `InputRef` type index is supported Now.
   * The index of `InputRef` is the column index of the primary table.
   */
  indexItem: ExprNode[];
  originalColumns: number[];
}

/** See `TableCatalog` struct in frontend crate for more information. */
export interface Table {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  columns: ColumnCatalog[];
  pk: ColumnOrder[];
  dependentRelations: number[];
  optionalAssociatedSourceId?: { $case: "associatedSourceId"; associatedSourceId: number };
  tableType: Table_TableType;
  distributionKey: number[];
  /** pk_indices of the corresponding materialize operator's output. */
  streamKey: number[];
  appendOnly: boolean;
  owner: number;
  properties: { [key: string]: string };
  fragmentId: number;
  /**
   * an optional column index which is the vnode of each row computed by the
   * table's consistent hash distribution
   */
  vnodeColIndex:
    | ColumnIndex
    | undefined;
  /**
   * An optional column index of row id. If the primary key is specified by users,
   * this will be `None`.
   */
  rowIdIndex:
    | ColumnIndex
    | undefined;
  /**
   * The column indices which are stored in the state store's value with
   * row-encoding. Currently is not supported yet and expected to be
   * `[0..columns.len()]`.
   */
  valueIndices: number[];
  definition: string;
  handlePkConflict: boolean;
  readPrefixLenHint: number;
}

export const Table_TableType = {
  UNSPECIFIED: "UNSPECIFIED",
  TABLE: "TABLE",
  MATERIALIZED_VIEW: "MATERIALIZED_VIEW",
  INDEX: "INDEX",
  INTERNAL: "INTERNAL",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Table_TableType = typeof Table_TableType[keyof typeof Table_TableType];

export function table_TableTypeFromJSON(object: any): Table_TableType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return Table_TableType.UNSPECIFIED;
    case 1:
    case "TABLE":
      return Table_TableType.TABLE;
    case 2:
    case "MATERIALIZED_VIEW":
      return Table_TableType.MATERIALIZED_VIEW;
    case 3:
    case "INDEX":
      return Table_TableType.INDEX;
    case 4:
    case "INTERNAL":
      return Table_TableType.INTERNAL;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Table_TableType.UNRECOGNIZED;
  }
}

export function table_TableTypeToJSON(object: Table_TableType): string {
  switch (object) {
    case Table_TableType.UNSPECIFIED:
      return "UNSPECIFIED";
    case Table_TableType.TABLE:
      return "TABLE";
    case Table_TableType.MATERIALIZED_VIEW:
      return "MATERIALIZED_VIEW";
    case Table_TableType.INDEX:
      return "INDEX";
    case Table_TableType.INTERNAL:
      return "INTERNAL";
    case Table_TableType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface Table_PropertiesEntry {
  key: string;
  value: string;
}

export interface View {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  owner: number;
  properties: { [key: string]: string };
  sql: string;
  dependentRelations: number[];
  /** User-specified column names. */
  columns: Field[];
}

export interface View_PropertiesEntry {
  key: string;
  value: string;
}

export interface Schema {
  id: number;
  databaseId: number;
  name: string;
  owner: number;
}

export interface Database {
  id: number;
  name: string;
  owner: number;
}

function createBaseColumnIndex(): ColumnIndex {
  return { index: 0 };
}

export const ColumnIndex = {
  fromJSON(object: any): ColumnIndex {
    return { index: isSet(object.index) ? Number(object.index) : 0 };
  },

  toJSON(message: ColumnIndex): unknown {
    const obj: any = {};
    message.index !== undefined && (obj.index = Math.round(message.index));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnIndex>, I>>(object: I): ColumnIndex {
    const message = createBaseColumnIndex();
    message.index = object.index ?? 0;
    return message;
  },
};

function createBaseStreamSourceInfo(): StreamSourceInfo {
  return {
    rowFormat: RowFormatType.ROW_UNSPECIFIED,
    rowSchemaLocation: "",
    useSchemaRegistry: false,
    protoMessageName: "",
    csvDelimiter: 0,
    csvHasHeader: false,
  };
}

export const StreamSourceInfo = {
  fromJSON(object: any): StreamSourceInfo {
    return {
      rowFormat: isSet(object.rowFormat) ? rowFormatTypeFromJSON(object.rowFormat) : RowFormatType.ROW_UNSPECIFIED,
      rowSchemaLocation: isSet(object.rowSchemaLocation) ? String(object.rowSchemaLocation) : "",
      useSchemaRegistry: isSet(object.useSchemaRegistry) ? Boolean(object.useSchemaRegistry) : false,
      protoMessageName: isSet(object.protoMessageName) ? String(object.protoMessageName) : "",
      csvDelimiter: isSet(object.csvDelimiter) ? Number(object.csvDelimiter) : 0,
      csvHasHeader: isSet(object.csvHasHeader) ? Boolean(object.csvHasHeader) : false,
    };
  },

  toJSON(message: StreamSourceInfo): unknown {
    const obj: any = {};
    message.rowFormat !== undefined && (obj.rowFormat = rowFormatTypeToJSON(message.rowFormat));
    message.rowSchemaLocation !== undefined && (obj.rowSchemaLocation = message.rowSchemaLocation);
    message.useSchemaRegistry !== undefined && (obj.useSchemaRegistry = message.useSchemaRegistry);
    message.protoMessageName !== undefined && (obj.protoMessageName = message.protoMessageName);
    message.csvDelimiter !== undefined && (obj.csvDelimiter = Math.round(message.csvDelimiter));
    message.csvHasHeader !== undefined && (obj.csvHasHeader = message.csvHasHeader);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSourceInfo>, I>>(object: I): StreamSourceInfo {
    const message = createBaseStreamSourceInfo();
    message.rowFormat = object.rowFormat ?? RowFormatType.ROW_UNSPECIFIED;
    message.rowSchemaLocation = object.rowSchemaLocation ?? "";
    message.useSchemaRegistry = object.useSchemaRegistry ?? false;
    message.protoMessageName = object.protoMessageName ?? "";
    message.csvDelimiter = object.csvDelimiter ?? 0;
    message.csvHasHeader = object.csvHasHeader ?? false;
    return message;
  },
};

function createBaseSource(): Source {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    rowIdIndex: undefined,
    columns: [],
    pkColumnIds: [],
    properties: {},
    owner: 0,
    info: undefined,
  };
}

export const Source = {
  fromJSON(object: any): Source {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      rowIdIndex: isSet(object.rowIdIndex) ? ColumnIndex.fromJSON(object.rowIdIndex) : undefined,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pkColumnIds: Array.isArray(object?.pkColumnIds) ? object.pkColumnIds.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      info: isSet(object.info) ? StreamSourceInfo.fromJSON(object.info) : undefined,
    };
  },

  toJSON(message: Source): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.rowIdIndex !== undefined &&
      (obj.rowIdIndex = message.rowIdIndex ? ColumnIndex.toJSON(message.rowIdIndex) : undefined);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pkColumnIds) {
      obj.pkColumnIds = message.pkColumnIds.map((e) => Math.round(e));
    } else {
      obj.pkColumnIds = [];
    }
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    message.info !== undefined && (obj.info = message.info ? StreamSourceInfo.toJSON(message.info) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Source>, I>>(object: I): Source {
    const message = createBaseSource();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.rowIdIndex = (object.rowIdIndex !== undefined && object.rowIdIndex !== null)
      ? ColumnIndex.fromPartial(object.rowIdIndex)
      : undefined;
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pkColumnIds = object.pkColumnIds?.map((e) => e) || [];
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.owner = object.owner ?? 0;
    message.info = (object.info !== undefined && object.info !== null)
      ? StreamSourceInfo.fromPartial(object.info)
      : undefined;
    return message;
  },
};

function createBaseSource_PropertiesEntry(): Source_PropertiesEntry {
  return { key: "", value: "" };
}

export const Source_PropertiesEntry = {
  fromJSON(object: any): Source_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Source_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Source_PropertiesEntry>, I>>(object: I): Source_PropertiesEntry {
    const message = createBaseSource_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSink(): Sink {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    columns: [],
    pk: [],
    dependentRelations: [],
    distributionKey: [],
    streamKey: [],
    appendOnly: false,
    owner: 0,
    properties: {},
    definition: "",
  };
}

export const Sink = {
  fromJSON(object: any): Sink {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      distributionKey: Array.isArray(object?.distributionKey)
        ? object.distributionKey.map((e: any) => Number(e))
        : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      appendOnly: isSet(object.appendOnly) ? Boolean(object.appendOnly) : false,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      definition: isSet(object.definition) ? String(object.definition) : "",
    };
  },

  toJSON(message: Sink): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
    } else {
      obj.streamKey = [];
    }
    message.appendOnly !== undefined && (obj.appendOnly = message.appendOnly);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.definition !== undefined && (obj.definition = message.definition);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Sink>, I>>(object: I): Sink {
    const message = createBaseSink();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.appendOnly = object.appendOnly ?? false;
    message.owner = object.owner ?? 0;
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.definition = object.definition ?? "";
    return message;
  },
};

function createBaseSink_PropertiesEntry(): Sink_PropertiesEntry {
  return { key: "", value: "" };
}

export const Sink_PropertiesEntry = {
  fromJSON(object: any): Sink_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Sink_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Sink_PropertiesEntry>, I>>(object: I): Sink_PropertiesEntry {
    const message = createBaseSink_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseIndex(): Index {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    owner: 0,
    indexTableId: 0,
    primaryTableId: 0,
    indexItem: [],
    originalColumns: [],
  };
}

export const Index = {
  fromJSON(object: any): Index {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      indexTableId: isSet(object.indexTableId) ? Number(object.indexTableId) : 0,
      primaryTableId: isSet(object.primaryTableId) ? Number(object.primaryTableId) : 0,
      indexItem: Array.isArray(object?.indexItem)
        ? object.indexItem.map((e: any) => ExprNode.fromJSON(e))
        : [],
      originalColumns: Array.isArray(object?.originalColumns) ? object.originalColumns.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: Index): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    message.indexTableId !== undefined && (obj.indexTableId = Math.round(message.indexTableId));
    message.primaryTableId !== undefined && (obj.primaryTableId = Math.round(message.primaryTableId));
    if (message.indexItem) {
      obj.indexItem = message.indexItem.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.indexItem = [];
    }
    if (message.originalColumns) {
      obj.originalColumns = message.originalColumns.map((e) => Math.round(e));
    } else {
      obj.originalColumns = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Index>, I>>(object: I): Index {
    const message = createBaseIndex();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    message.indexTableId = object.indexTableId ?? 0;
    message.primaryTableId = object.primaryTableId ?? 0;
    message.indexItem = object.indexItem?.map((e) => ExprNode.fromPartial(e)) || [];
    message.originalColumns = object.originalColumns?.map((e) => e) || [];
    return message;
  },
};

function createBaseTable(): Table {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    columns: [],
    pk: [],
    dependentRelations: [],
    optionalAssociatedSourceId: undefined,
    tableType: Table_TableType.UNSPECIFIED,
    distributionKey: [],
    streamKey: [],
    appendOnly: false,
    owner: 0,
    properties: {},
    fragmentId: 0,
    vnodeColIndex: undefined,
    rowIdIndex: undefined,
    valueIndices: [],
    definition: "",
    handlePkConflict: false,
    readPrefixLenHint: 0,
  };
}

export const Table = {
  fromJSON(object: any): Table {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      optionalAssociatedSourceId: isSet(object.associatedSourceId)
        ? { $case: "associatedSourceId", associatedSourceId: Number(object.associatedSourceId) }
        : undefined,
      tableType: isSet(object.tableType) ? table_TableTypeFromJSON(object.tableType) : Table_TableType.UNSPECIFIED,
      distributionKey: Array.isArray(object?.distributionKey)
        ? object.distributionKey.map((e: any) => Number(e))
        : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      appendOnly: isSet(object.appendOnly) ? Boolean(object.appendOnly) : false,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      vnodeColIndex: isSet(object.vnodeColIndex) ? ColumnIndex.fromJSON(object.vnodeColIndex) : undefined,
      rowIdIndex: isSet(object.rowIdIndex) ? ColumnIndex.fromJSON(object.rowIdIndex) : undefined,
      valueIndices: Array.isArray(object?.valueIndices)
        ? object.valueIndices.map((e: any) => Number(e))
        : [],
      definition: isSet(object.definition) ? String(object.definition) : "",
      handlePkConflict: isSet(object.handlePkConflict) ? Boolean(object.handlePkConflict) : false,
      readPrefixLenHint: isSet(object.readPrefixLenHint) ? Number(object.readPrefixLenHint) : 0,
    };
  },

  toJSON(message: Table): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    message.optionalAssociatedSourceId?.$case === "associatedSourceId" &&
      (obj.associatedSourceId = Math.round(message.optionalAssociatedSourceId?.associatedSourceId));
    message.tableType !== undefined && (obj.tableType = table_TableTypeToJSON(message.tableType));
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
    } else {
      obj.streamKey = [];
    }
    message.appendOnly !== undefined && (obj.appendOnly = message.appendOnly);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.vnodeColIndex !== undefined &&
      (obj.vnodeColIndex = message.vnodeColIndex ? ColumnIndex.toJSON(message.vnodeColIndex) : undefined);
    message.rowIdIndex !== undefined &&
      (obj.rowIdIndex = message.rowIdIndex ? ColumnIndex.toJSON(message.rowIdIndex) : undefined);
    if (message.valueIndices) {
      obj.valueIndices = message.valueIndices.map((e) => Math.round(e));
    } else {
      obj.valueIndices = [];
    }
    message.definition !== undefined && (obj.definition = message.definition);
    message.handlePkConflict !== undefined && (obj.handlePkConflict = message.handlePkConflict);
    message.readPrefixLenHint !== undefined && (obj.readPrefixLenHint = Math.round(message.readPrefixLenHint));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table>, I>>(object: I): Table {
    const message = createBaseTable();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    if (
      object.optionalAssociatedSourceId?.$case === "associatedSourceId" &&
      object.optionalAssociatedSourceId?.associatedSourceId !== undefined &&
      object.optionalAssociatedSourceId?.associatedSourceId !== null
    ) {
      message.optionalAssociatedSourceId = {
        $case: "associatedSourceId",
        associatedSourceId: object.optionalAssociatedSourceId.associatedSourceId,
      };
    }
    message.tableType = object.tableType ?? Table_TableType.UNSPECIFIED;
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.appendOnly = object.appendOnly ?? false;
    message.owner = object.owner ?? 0;
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.fragmentId = object.fragmentId ?? 0;
    message.vnodeColIndex = (object.vnodeColIndex !== undefined && object.vnodeColIndex !== null)
      ? ColumnIndex.fromPartial(object.vnodeColIndex)
      : undefined;
    message.rowIdIndex = (object.rowIdIndex !== undefined && object.rowIdIndex !== null)
      ? ColumnIndex.fromPartial(object.rowIdIndex)
      : undefined;
    message.valueIndices = object.valueIndices?.map((e) => e) || [];
    message.definition = object.definition ?? "";
    message.handlePkConflict = object.handlePkConflict ?? false;
    message.readPrefixLenHint = object.readPrefixLenHint ?? 0;
    return message;
  },
};

function createBaseTable_PropertiesEntry(): Table_PropertiesEntry {
  return { key: "", value: "" };
}

export const Table_PropertiesEntry = {
  fromJSON(object: any): Table_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Table_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table_PropertiesEntry>, I>>(object: I): Table_PropertiesEntry {
    const message = createBaseTable_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseView(): View {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    owner: 0,
    properties: {},
    sql: "",
    dependentRelations: [],
    columns: [],
  };
}

export const View = {
  fromJSON(object: any): View {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      sql: isSet(object.sql) ? String(object.sql) : "",
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      columns: Array.isArray(object?.columns)
        ? object.columns.map((e: any) => Field.fromJSON(e))
        : [],
    };
  },

  toJSON(message: View): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.sql !== undefined && (obj.sql = message.sql);
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<View>, I>>(object: I): View {
    const message = createBaseView();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.sql = object.sql ?? "";
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    message.columns = object.columns?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseView_PropertiesEntry(): View_PropertiesEntry {
  return { key: "", value: "" };
}

export const View_PropertiesEntry = {
  fromJSON(object: any): View_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: View_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<View_PropertiesEntry>, I>>(object: I): View_PropertiesEntry {
    const message = createBaseView_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSchema(): Schema {
  return { id: 0, databaseId: 0, name: "", owner: 0 };
}

export const Schema = {
  fromJSON(object: any): Schema {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
    };
  },

  toJSON(message: Schema): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Schema>, I>>(object: I): Schema {
    const message = createBaseSchema();
    message.id = object.id ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    return message;
  },
};

function createBaseDatabase(): Database {
  return { id: 0, name: "", owner: 0 };
}

export const Database = {
  fromJSON(object: any): Database {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
    };
  },

  toJSON(message: Database): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Database>, I>>(object: I): Database {
    const message = createBaseDatabase();
    message.id = object.id ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    return message;
  },
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends { $case: string } ? { [K in keyof Omit<T, "$case">]?: DeepPartial<T[K]> } & { $case: T["$case"] }
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
