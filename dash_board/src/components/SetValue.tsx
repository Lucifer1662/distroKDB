import { Button, LinearProgress, TextField, Typography } from "@suid/material";
import { For, createResource, createSignal } from "solid-js";
import { createStore } from "solid-js/store";

interface NodeInformation {
    id: number,
    isActive: boolean,
    position: number
}

interface Props {
    setValue: (params: { key: string, value: string }) => Promise<boolean>
}

export function SetValue(props: Props) {
    const [store, set_store] = createStore({ key: "", value: "" });
    const [operation, set_operation] = createStore({ key: "", value: "" });
    const [success, { mutate, refetch }] = createResource(operation, async ({ key, value }) => {
        if (key) {
            return props.setValue({ key, value })
        } else {
            return undefined;
        }
    });
    const set_value = () => { mutate(); set_operation(store); refetch(); }

    return (
        <div>
            <Typography>Set value</Typography>
            <table >
                <tbody>
                    <tr>
                        <td>Key</td>
                        <td>Value</td>
                    </tr>
                    <tr>
                        <td><TextField variant="standard" value={store.key} onChange={(e) => set_store((prev) => ({ ...prev, key: e.target.value }))} /></td>
                        <td><TextField variant="standard" value={store.value} onChange={(e) => set_store((prev) => ({ ...prev, value: e.target.value }))} /></td>
                    </tr>
                    <tr>
                        <td><Button onClick={() => set_value()} >Submit</Button></td>
                        <td>{success() !== undefined ? (success() ? "Successful" : "Failed") : (success.loading ? <LinearProgress /> : "")}</td>
                    </tr>
                </tbody>
            </table>
        </div >
    );
};