import { GetValue as StoryComponent } from "../components/GetValue"
import type { Meta, StoryFn } from "@storybook/html";
import type { ComponentProps } from "solid-js";



function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


const Template = ((args: any) => <StoryComponent
    {...args}
    getValue={async (key: string) => {
        await delay(args.loading_time)
        return args.data[key]
    }}
/>) as StoryFn<ComponentProps<typeof StoryComponent>>;

export const GetValueTemplate = Template.bind({});


GetValueTemplate.args = {
    //@ts-ignore
    loading_time: 1000,

    //@ts-ignore
    data: {
        "foo": "foo",
        "bar": "haha"
    },
};




export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Get Value",
    tags: ["autodocs"],
    argTypes: {
        loading_time: { control: "number", description: "The nodes" },
        data: { control: "object", description: "The nodes" },
    }
} as Meta<ComponentProps<typeof StoryComponent>>;