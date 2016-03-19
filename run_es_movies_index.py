__author__ = 'hannahprovenza'


import web
from web import form
import build_es_movies_index as b

render = web.template.render('templates/')

urls = ('/', 'index',
        '/results', 'results',
        "/totalstuff", 'totalstuff',
        "/daterange", "daterange",
        "/fieldsearch", 'fieldsearch',
        "/mwsearch", "mwsearch",
        "/phrasesearch", "phrasesearch")

app = web.application(urls, globals())

movieix = b.build_es_movies_index()

fields = ["title", "language", "country", "director", "location", "starring", "text", "categories", "time", "runtime"]

totalsize = form.Form(form.Button("Total size of index"))
date_range = form.Form(form.Textbox("Lower bound of date range:"), form.Textbox("Upper bound of date range:"))
field_search = form.Form(form.Dropdown(name="Fields", args=fields), form.Textbox("Text to search in field:"))
multiword = form.Form(form.Textbox("Multiword search:"))
phrase = form.Form(form.Textbox("Phrase search:"))
field_combo = form.Form(form.Textbox("Multifield search:"))



class index:
    def GET(self):
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        return render.page_1(form1, form2, form3, form4, form5, form6)

class totalstuff:
    def GET(self):
        '''
        Gets the total number of documents
        :return: a results page
        '''
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        if form1.validates():
            res = movieix.q_total()
            return render.page_2(res, form1, form2, form3, form4, form5, form6)

class daterange:
    def GET(self):
        '''
        Gets the number of films within a date range
        :return: a results page
        '''
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        if form2.validates():
            res = movieix.q_time_range(str(form2["Lower bound of date range:"].value), str(form2["Upper bound of date range:"].value))
            return render.page_3(res, form1, form2, form3, form4, form5, form6)

class fieldsearch:
    def GET(self):
        '''
        Searches for a term within a specific field
        :return: a results page
        '''
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        if form3.validates():
            res = movieix.q_field(str(form3["Fields"].value), str(form3["Text to search in field:"].value))
            return render.page_4(res, form1, form2, form3, form4, form5, form6)

class mwsearch:
    def GET(self):
        '''
        Multiword search!
        :return: a results page!
        '''
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        if form4.validates():
            res = movieix.q_mw(str(form4["Multiword search:"].value))
            return render.page_4(res, form1, form2, form3, form4, form5, form6)


class phrasesearch:
    def GET(self):
        '''
        Phrase search!
        :return: another results page!
        '''
        form1 = totalsize()
        form2 = date_range()
        form3 = field_search()
        form4 = multiword()
        form5 = phrase()
        form6 = field_combo()
        if form5.validates():
            res = movieix.q_phr(str(form5["Phrase search:"].value))
            return render.page_4(res, form1, form2, form3, form4, form5, form6)

if __name__ == "__main__":
    web.internalerror = web.debugerror
    app.run()
