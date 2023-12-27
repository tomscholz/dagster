import graphene


class GrapheneAssetSelection(graphene.ObjectType):
    assetSelectionString = graphene.String()
