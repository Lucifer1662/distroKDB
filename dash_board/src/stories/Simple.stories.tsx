import type { Meta, StoryFn } from "@storybook/html";
import type { ComponentProps } from "solid-js";

function Simple() {
    return <div>hello world</div>
}


// Simple examples
export const SimpleDefault = () => <Simple />;

export default {
    /* 👇 The title prop is optional.
     * See https://storybook.js.org/docs/html/configure/overview#configure-story-loading
     * to learn how to generate automatic titles
     */
    title: "Simple",
    argTypes: {
        initialValue: { control: "number" },
    },
} as Meta<ComponentProps<typeof Simple>>;