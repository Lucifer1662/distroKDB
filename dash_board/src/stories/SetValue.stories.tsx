import { SetValue as StoryComponent } from "../components/SetValue"
import type { Meta, StoryFn } from "@storybook/html";
import { ComponentProps, createSignal } from "solid-js";

function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function Parent(args: any) {
    const [data, setData] = createSignal(args.initial_data)

    return <div> <StoryComponent
        {...args}
        setValue={async ({ key, value }) => {
            console.log("here")
            await delay(args.loading_time)
            if (args.should_succeed) {
                setData((prev: any) => ({ ...prev, [key]: value }));
            }
            return args.should_succeed;
        }} />
        {JSON.stringify(data())}
    </div>
}


const Template = ((args: any) => <Parent {...args} />) as StoryFn<ComponentProps<typeof StoryComponent>>;

export const SetValueSucceed = Template.bind({});
SetValueSucceed.args = {
    //@ts-ignore
    loading_time: 1000,

    //@ts-ignore
    should_succeed: true,

    //@ts-ignore
    initial_data: {
        "foo": "foo",
        "bar": "haha"
    },
};

export const SetValueFails = Template.bind({});
SetValueFails.args = {
    //@ts-ignore
    loading_time: 1000,

    //@ts-ignore
    should_succeed: false,

    //@ts-ignore
    initial_data: {
        "foo": "foo",
        "bar": "haha"
    },
};





export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Set Value",
    tags: ["autodocs"],
    argTypes: {
        loading_time: { control: "number", description: "The nodes" },
        should_succeed: { control: "boolean", description: "Should the operation succeed" },
        initial_data: { control: "object", description: "The nodes" },
    }
} as Meta<ComponentProps<typeof StoryComponent>>;