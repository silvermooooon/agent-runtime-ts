import type { OAuthAuth } from "../types.ts";

const importOAuthModule = (specifier: string): Promise<unknown> => {
	const runtimeSpecifier = import.meta.url.endsWith(".js") ? specifier.replace(/\.ts$/, ".js") : specifier;
	return import(new URL(runtimeSpecifier, import.meta.url).href);
};

type OAuthFlowLoaders = {
	openaiChatGPT: () => OAuthAuth | Promise<OAuthAuth>;
};

let bundledLoaders: OAuthFlowLoaders | undefined;

export function registerBundledOAuthFlowLoaders(loaders: OAuthFlowLoaders): void {
	bundledLoaders = loaders;
}

export const loadOpenAIChatGPTOAuth = async (): Promise<OAuthAuth> => {
	if (bundledLoaders) return bundledLoaders.openaiChatGPT();
	return ((await importOAuthModule("./openai-chatgpt.ts")) as { openaiChatGPTOAuth: OAuthAuth }).openaiChatGPTOAuth;
};
