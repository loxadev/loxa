import { Children, Fragment, isValidElement, type ReactNode } from "react";

function hasRenderableContent(node: ReactNode): boolean {
  return Children.toArray(node).some((child) => {
    if (isValidElement<{ children?: ReactNode }>(child) && child.type === Fragment) {
      return hasRenderableContent(child.props.children);
    }
    return typeof child !== "string" || child.length > 0;
  });
}

export { hasRenderableContent };
