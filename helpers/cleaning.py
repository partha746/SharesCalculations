"""Small text/number cleaning helpers."""


class DataCleaner:
    def convert_from_symbol(self, text):
        symbols = ["$", "₹", "?", ",", " "]
        
        for each_symbol in symbols:
            text = text.replace(each_symbol, '')      
        
        return text

