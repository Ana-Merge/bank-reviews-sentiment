export const getAllChildProducts = (productTree, productId) => {
    const findProductAndDirectChildren = (nodes, targetId) => {
        for (let node of nodes) {
            if (node.id === targetId) {
                if (node.children && node.children.length > 0) {
                    return node.children;
                }
                return [node];
            }
            if (node.children) {
                const found = findProductAndDirectChildren(node.children, targetId);
                if (found.length > 0) return found;
            }
        }
        return [];
    };

    return findProductAndDirectChildren(productTree, productId);
};

export const findProductInTree = (nodes, targetId) => {
    for (let node of nodes) {
        if (node.id === targetId) {
            return node;
        }
        if (node.children) {
            const found = findProductInTree(node.children, targetId);
            if (found) return found;
        }
    }
    return null;
};