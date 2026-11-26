import type { ClassifierModel, ImageModel, Model } from "../types.ts";
import { OPENAI_MODELS } from "./openai.models.ts";

function toAzureResponsesModel<TModel extends Model<"openai-responses"> & { type: "chat" }>(
	model: TModel,
): Model<"azure-openai-responses"> & { type: "chat"; id: TModel["id"]; provider: "azure" } {
	return {
		...model,
		api: "azure-openai-responses",
		provider: "azure",
		baseUrl: "",
	};
}

export const AZURE_MODELS = {
	"gpt-4o": toAzureResponsesModel(OPENAI_MODELS["gpt-4o"]),
	"gpt-4o-mini": toAzureResponsesModel(OPENAI_MODELS["gpt-4o-mini"]),
	"gpt-4.1": toAzureResponsesModel(OPENAI_MODELS["gpt-4.1"]),
	"gpt-4.1-mini": toAzureResponsesModel(OPENAI_MODELS["gpt-4.1-mini"]),
	"gpt-5": toAzureResponsesModel(OPENAI_MODELS["gpt-5"]),
	"gpt-5-mini": toAzureResponsesModel(OPENAI_MODELS["gpt-5-mini"]),
	"gpt-5.4": { ...toAzureResponsesModel(OPENAI_MODELS["gpt-5.4"]), contextWindow: 1050000 },
	"gpt-5.4-mini": toAzureResponsesModel(OPENAI_MODELS["gpt-5.4-mini"]),
	o3: toAzureResponsesModel(OPENAI_MODELS.o3),
	"o4-mini": toAzureResponsesModel(OPENAI_MODELS["o4-mini"]),
} as const satisfies Record<string, Model<"azure-openai-responses"> & { type: "chat"; provider: "azure" }>;

export const AZURE_IMAGE_MODELS: Record<string, ImageModel<never>> = {};

export const AZURE_CLASSIFIER_MODELS: Record<string, ClassifierModel<never>> = {};
