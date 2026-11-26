import { AZURE_CLASSIFIER_MODELS, AZURE_IMAGE_MODELS, AZURE_MODELS } from "./providers/azure.models.ts";
import { OPENAI_CLASSIFIER_MODELS, OPENAI_IMAGE_MODELS, OPENAI_MODELS } from "./providers/openai.models.ts";

export const MODELS: {
	readonly azure: typeof AZURE_MODELS;
	readonly openai: typeof OPENAI_MODELS;
} = {
	azure: AZURE_MODELS,
	openai: OPENAI_MODELS,
};

export const IMAGE_MODELS: {
	readonly azure: typeof AZURE_IMAGE_MODELS;
	readonly openai: typeof OPENAI_IMAGE_MODELS;
} = {
	azure: AZURE_IMAGE_MODELS,
	openai: OPENAI_IMAGE_MODELS,
};

export const CLASSIFIER_MODELS: {
	readonly azure: typeof AZURE_CLASSIFIER_MODELS;
	readonly openai: typeof OPENAI_CLASSIFIER_MODELS;
} = {
	azure: AZURE_CLASSIFIER_MODELS,
	openai: OPENAI_CLASSIFIER_MODELS,
};
