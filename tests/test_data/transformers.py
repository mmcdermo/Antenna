from antenna.Transformers import Transformer

class CustomTestTransformer(Transformer):
    """
    Consumes Items of given type
    Side Effects: None
    Produces: Items of the given type
    """
    def __init__(self, aws_manager, params):
        self._required_keywords = [
        ]
        self.input_item_types = "ArticleReference"
        self.output_item_types = "TransformedArticle"
        super(CustomTestTransformer, self).__init__(aws_manager, params)

    def transform(self, item):
        return Item(item_type="TransformedArticle",
                     payload=item.payload)
