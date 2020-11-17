import re

##########################################################################
# Regular expressions definition
##########################################################################
## Search Symbols
symbols = r'(?<!^)\s*[\"\<\+\/\:\.\,\'\-\(\[]+\s*'
## A-C
first = r'{}(\b(?<!the\s)An?(ima)?(lternate|glais|ci[óoÃ³n]{{1,2}}n|do|ted)\b|\bBl?o(ody|snian|nus)\b|\bC[oea][npiÃ]*\s*(llection\'?|(s[ou]b?)?([tiu]+(lo|Ã)|red)|material)s?\b).*$'.format(symbols)
## D-E
second= r'{}(\b(?<!the\s)(?<!\,\s)D[iuoa]*(cumentar(y|ies)|ba?[bl](a?d[oa]|ed?)|rector\'?s|gital|nsk|ppiato|vd\'?)s?\b|\bE[xnd]*\s*(dited|tended|glish|espa[nñ]ol|sp|\d{{1,4}})\b).*$'.format(symbols)
## F-I
third = r'{}(\b(?<!\.)F([uerie]*([l]+(men)?|ench(?!\sfor)|atured?|\)|stival\s+version(?=[\]\)])))?\b|\b(?<!\:)[SH](in)?Di?(?!\:)\b|\bIn\s*(ter(nationale?|sections?)|clu(ye|i)|color|englis|italian|spanish)\b).*$'.format(symbols)
## L-R
fourth= r'{}(\bL[ei]\s*(ve\s*((?=\))|action|movie|serie|version|collection|animat(ion|ed))s?|gendado|film)\b|\bM([eGi][Mtd]|ulticom|arathi|alayalam)\b|\bn(aughty|orsk)\b|\bO[rlm](iginal(fassung)?|ympus|u)\b|\bP[lor]+(tuguese|us|ima)\b|\bR[ea]((\-?ma)?(st[oe]r(ed)?|ked?)|ad|ted)\b).*$'.format(symbols)
## S
fifth = r'{}(\b(?<!with\s)S[oeapyui]*(nce|cial\s+(?!victims|disruption|delivery)|btit(ulad[oa]|le[sd]?)|b(bed)?(?!\szero)|omi|ng\-\s*along|ttotitoli|nchronisiert|(son|rie)s?\s*\d{{1,3}}(\-\d{{1,3}})?|\d{{1,4}})\b).*$'.format(symbols)
## T-V
sixth = r'{}(\bT[hae]+\s*(mil|legu|complete\s*(movie|serie)s?|atrical|kstitetty)\b|\b(?<!the\s)U[ln](edited|cut|rated|geschnittene|tertitelt|zensiert)\b|\b(?<!the\s)(?<!\')V[O\.l]*(ersione?|S\.?T|umes?|\s*\d{{1,3}}(?!\,)|s\.?|[FE])\b).*$'.format(symbols)
## Numbers/Empty Symbols
seventh=r'{}(\bmovie(?=\scollection)\b|\b(?<!the\s)\d{{1,4}}th(?!\s(\d{{4}}|anniversary))\b|\b\dd\b|(?<=[\(\[])\b[1-2][0-9][0-9][0-9]\b).*$'.format(symbols)
## Without Symbols
eighth = r'(?<!^)(?<!:)\s+(\bPrima\s*Visione\b|\bL[ae]\s*(film|pel[Ãi]\s*cula)\b|\bThe\s*(movie|film|series?)(?=\s\:)|\b(?<!The\s)(?<!\d\s)\d{1,3}[\s\-\&]\d{1,3}\b(?!.)).*$'

## Skip matches
# skip_one = r'{}(\bLive\s*(from|at|[io](ver|n)|\&|~|\,|\(|\:|\-|country|with|and|[1-2][0-9][0-9][0-9]|sometime))'.format(symbols)

##########################################################################
# Compiling regular expressions
##########################################################################
regex_title_first   = re.compile(r'{}'.format(first), re.I)
regex_title_second  = re.compile(r'{}'.format(second), re.I)
regex_title_third   = re.compile(r'{}'.format(third), re.I)
regex_title_fourth  = re.compile(r'{}'.format(fourth), re.I)
regex_title_fifth   = re.compile(r'{}'.format(fifth), re.I)
regex_title_sixth   = re.compile(r'{}'.format(sixth), re.I)
regex_title_seventh = re.compile(r'{}'.format(seventh), re.I)
regex_title_eighth  = re.compile(r'{}'.format(eighth), re.I)
# regex_skip_one      = re.compile(r'{}'.format(skip_one), re.I)

def clean_title(content_title):
    content_title = content_title.strip()
    content_title = regex_title_first.sub('', content_title)
    content_title = regex_title_second.sub('', content_title)
    content_title = regex_title_third.sub('', content_title)
    # if not regex_skip_one.search(content_title):
    content_title = regex_title_fourth.sub('', content_title)
    content_title = regex_title_fifth.sub('', content_title)
    content_title = regex_title_sixth.sub('', content_title)
    content_title = regex_title_seventh.sub('', content_title)
    content_title = regex_title_eighth.sub('', content_title)
    content_title = content_title.replace('(LAS)', '')
    content_title = content_title.replace('\"', '')

    for symbol in "+/:.\\,\'-(\"[":
        content_title = content_title[:-1] if content_title[-1] == symbol and symbol not in content_title[-5:] else content_title

    return content_title.strip()
