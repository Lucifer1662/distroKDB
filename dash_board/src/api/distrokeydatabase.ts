import { createResource, Resource } from "solid-js";

export type Address = string;


interface NodeConfig {
    id: number,
    address: string
}

const nodes : NodeConfig[] = [
    {id:0, address:"http://localhost:3001/"},
    {id:1, address:"http://localhost:3002/"}
]


function getRandomInt(max:number) {
    return Math.floor(Math.random() * max);
}

function random_node_id(){
    return nodes[getRandomInt(nodes.length)].id
}

function API_PATH(id: number){
    const index = nodes.findIndex((node)=>node.id==id)
    if(index !== -1){
        return nodes[index].address
    }
    console.error("Could not find node with id " + id)
    return ""
}


type APIMethod = "POST" | "UPDATE" | "GET" | "DELETE" | "PATCH";

type APIParams = {
    method: APIMethod,
    uri: string,
    queryParams?: any,
    body?: any
    bodyFormData?: any,
};

async function api<T = any>(params: APIParams, ...args: any[]): Promise<{data:T | undefined, succeeded: boolean}> {
    let url = new URL(params.uri);

    Object.keys(params.queryParams || {}).forEach(key => {
        url.searchParams.set(key, params.queryParams[key] + '');
    })

    let headers = new Headers();

    let request: RequestInit = {
        method: params.method,
        headers: headers
    };



    if (params.body !== undefined) {
        request.body = JSON.stringify(params.body);
        headers.set('Content-Type', 'application/json');
    } else if (params.bodyFormData) {
        request.body = params.bodyFormData;
        //do not set content headers
    }

    try {
        const res = await fetch(url.toString(), request);
        try{
            if(res.ok){
                const data = await res.json();
                return {data: data as T, succeeded: true};
            }else{
                return {data:undefined, succeeded: false};
            }
        }catch(e){
            return {data:undefined, succeeded: true}
        }
    } catch (e) {
        return {data:undefined, succeeded: false}
    }

}

export namespace ObjectOps {
    const RESOURCE_API = (key: string, node_id:number = random_node_id()) => API_PATH(node_id) + "/objects/" + key;
    export async function add(key: string, value: string) { return await (await api({ method: "POST", uri: RESOURCE_API(key), body: value })).succeeded; }
    export async function get(key: string) { return await (await api<string|undefined>({ method: "GET", uri: RESOURCE_API(key) })).data; }
}

export namespace ObjectsOps {
    const RESOURCE_API = (node_id:number) => API_PATH(node_id) + "/objects/";
    export async function get_all_local(node_id:number) { return await (await api<AllLocalResponse>({ method: "GET", uri: RESOURCE_API(node_id) })).data; }
}


export interface AllLocalResponse {
    permanent_values: { [key:string]: string },
    temporary_values: { [key:string]: string }
}



export interface NodeInformation {
    id: number,
    isActive: boolean,
    position: number
  }

export namespace NodeOps {
    const RESOURCE_API = (node_id: number) => API_PATH(node_id) + "/objects"
    export async function get_node_ids() { return nodes.map((node)=>node.id); }
    export async function get_node_information(node_id:number) {return (await api<NodeInformation>({ method: "GET", uri: RESOURCE_API(node_id) })).data; }
}


