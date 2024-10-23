/** Types generated for queries found in "queries/test_queries.sql" */
import { PreparedQuery } from '@pgtyped/runtime';

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

/** 'GetAllDevices' parameters type */
export type IGetAllDevicesParams = void;

/** 'GetAllDevices' return type */
export interface IGetAllDevicesResult {
  data: Json | null;
  device_id: string | null;
  name: string | null;
  tenant_id: string | null;
}

/** 'GetAllDevices' query type */
export interface IGetAllDevicesQuery {
  params: IGetAllDevicesParams;
  result: IGetAllDevicesResult;
}

const getAllDevicesIR: any = {"usedParamSet":{},"params":[],"statement":"select * from device_service.device_remote"};

/**
 * Query generated from SQL:
 * ```
 * select * from device_service.device_remote
 * ```
 */
export const getAllDevices = new PreparedQuery<IGetAllDevicesParams,IGetAllDevicesResult>(getAllDevicesIR);


