import { Button, TextField, Typography } from "@suid/material";
import { For, createResource, createSignal } from "solid-js";

interface NodeInformation {
    id: number,
    isActive: boolean,
    position: number
}

interface Props {
    getValue: (key: string) => Promise<string | undefined>
}

export function GetValue(props: Props) {
    const [key, set_key] = createSignal("");
    const [search_key, set_search_key] = createSignal("");
    const [value, { mutate }] = createResource(search_key, props.getValue);

    const get_value = (key: string) => { mutate(); set_search_key(key); }

    return (
        <div>
            <Typography>Get value</Typography>
            <table >
                <tbody>
                    <tr>
                        <td>Key</td>
                        <td>Value</td>
                    </tr>
                    <tr>
                        <td><TextField variant="standard" onChange={(e) => get_value(e.target.value)} /></td>
                        <td>{value() ? '"' + value() + '"' : (value.loading ? "Loading" : "null")}</td>
                    </tr>
                </tbody>
            </table>
        </div >
    );
};