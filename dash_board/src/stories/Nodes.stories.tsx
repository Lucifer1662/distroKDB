import { Nodes as StoryComponent } from "../components/Nodes";
import type { Meta, StoryFn } from "@storybook/html";
import type { ComponentProps } from "solid-js";

const Template = ((args: any) => <StoryComponent {...args} onClickNode={(node) => alert("Click:" + JSON.stringify(node))} />) as StoryFn<ComponentProps<typeof StoryComponent>>;
export const CounterTemplate = Template.bind({});
CounterTemplate.args = {
    nodes: [
        {
            id: 0, isActive: true, position: 12304023401234
        }, {
            id: 1, isActive: false, position: 74023490242344
        }
    ]
};




export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Nodes",
    tags: ["autodocs"],
    argTypes: {
        nodes: { control: "object", description: "The nodes" },
    }
} as Meta<ComponentProps<typeof StoryComponent>>;