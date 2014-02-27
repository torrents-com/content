#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re

##########################################
# * infohash
# * size
# * description
# * title
# * tags
# * quality
# * genre
# * season
# * episode
# * language
##########################################

# generic tags
tags = {
    "category" : ["movie", "movies", "series", "porn", "adult", "xxx", "tv", "tv shows", "anime", "video", "music", "game", "games", "software", "applications", 
                "books", "ebooks", "mobile", "picture", "image", "videos", "other", "others"]
    ,"genre_v" : ["action", "adult", "comedy", "crime", "drama", "thriller", "horror", "documentary", "sci-fi", "scifi", "sci fi", "romance",
            "fantasy", "family", "mystery", "adventure", "reality", "cartoons", "animation", "biography"]
    ,"genre_a" : ["blues", "rock", "pop", "trance", "rap", "electronic", "alternative", "hiphop", "hip hop", "hip-hop", "metal", "soundtrack", "house",
             "dance", "instrumental", "jazz", "techno", "punk", "reggae", "drum", "bass", "ambient", "karaoke", "folk", "classic", "religious"]
    ,"genre_b" : ["comic", "magazines", "academic", "audiobook"]
    ,"genre_x" : ["xxx", "hardcore", "anal", "blowjob", "brunette", "boobs", "hentai", "porn", "toriblack", "oral", "sex", "interracial",
        "threesome", "fetish", "blonde", "lesbian", "homemade", "gangbang", "creampie", "cream pie", "lesbians", "squirting", "cuckold", "stockings", "cumshots",
        "milf", "masturbation", "swingers" "boobs" "penetration", "handjob", "tits", "pov", "pornstar", "brunette", "interracial", "bukkake",
        "transsexual", "shemale", "bareback"]
    ,"language" : ["en", "it", "hi", "ja", "fr", "es", "ch", "de", "english", "japanese", "chinese", "french", "german", "hindi", "italian", "spanish", "ita", "dutch"]
    ,"quality_v" : ["hd", "720", "720p","1080", "1080p", "480p", "dvdrip" , "dvdr", "dvd", "hdrip", "highres", "xvid", "3d", "cam", "screener"]
    ,"quality_a" : ["mp3", "flac"]
    ,"other" : ["full_album", "wallpaper"]
    ,"subcategory" : ["windows", "pc", "console", "mobile", "android", "mac", "psp", "linux", "dreamcast", "wii", "ps2", "ps3", "xbox", 
                    "wallpaper", "wallpapers", "bollywood", "comics", "highres movies", "lossless", "audio books", "hd video", "articles", 
                    "educational", "religion", "manuals", "documentaries", "pdf", "patches", "fixes", "ios", "gamecube", "psx", "handheld",
                    "xbox360", "roms", "1280 x 1024", "1600 x 1200", "1366 x 768", "ipad", "apps", "iphone", "phones", "shows", "keygen"]
}


category_schema = {
    "movie" : "video", 
    "movies" : "video", 
    "series" : "video", 
    "porn": "video", 
    "adult": "video", 
    "xxx" : "video", 
    "tv" : "video", 
    "tv shows" : "video", 
    "anime" : "video", 
    "video" : "video", 
    "videos":"video", 
    "music":"audio", 
    "game":"application", 
    "games":"application", 
    "software":"application", 
    "applications":"application", 
    "books":"book", 
    "ebooks":"book", 
    "mobile":"application", 
    "picture":"image", 
    "image":"image", 
    "other":"torrent", 
    "others":"torrent"
}

synonyms = {
    "adult": ["xxx", "movie"],
    "porn": "xxx",
    "applications": "software",
    "game": "software",
    "books": "book",
    "movie": "series",
    "games": "game",
    "movies": "movie"
}




subcategory_category = {
            "windows": "software"
            , "pc": "software"
            , "console": "game"
            , "mobile": "mobile"
            , "android": "mobile"
            , "mac": "software"
            , "psp": "game"
            , "linux": "game"
            , "dreamcast": "game"
            , "wii": "game"
            , "ps2": "game"
            , "ps3": "game"
            , "xbox": "game"
            , "wallpaper": "image"
            , "wallpapers": "image"
            , "bollywood": "movie"
            , "comics": "book"
            , "highres movies": "movie"
            , "lossless":  "music"
            , "audio books": "books"
            , "hd video": "movie"
                }

def get_tags(category):
    cat2tag = [
                ('movie', 'movie'),
                ('movies', 'movie'),
                ('dvdr', 'movie'),
                ('documentary', 'documentary'),
                ('tv', 'series'),
                ('anime', 'anime'),
                ('animations', 'anime'),
                ('software', 'software'),
                ('applications', 'software'),
                ('mac', 'mac'),
                ('linux', 'linux'),
                ('unix', 'linux'),
                ('pc', 'windows'),
                ('windows', 'windows'),
                ('xxx', 'porn'),
                ('hentai', 'porn'),
                ('adult', 'porn'),
                ('books', 'ebook'),
                ('ebook', 'ebook'),
                ('audio books', 'audiobook'),
                ('games', 'game'),
                ('music', 'music'),
                ('Mobile', 'mobile'),
                ('graphic novels', 'comic'),
                ('hd', 'hd'),
                ('3d', '3d'),
                ('karaoke', 'karaoke'),
                ]
    
    return ','.join(tag for cat,tag in cat2tag if cat in category.lower())
    
    
                

def get_schema(data):
    "try to get the schema with any metadata"
    
    if 'category' in data and data['category'] in category_schema:
        #direct relation for the category
        return category_schema[data['category']]
    
    if 'genre' in data:
        # it seeks in the genres
        if data['genre'] in tags['genre_v'] or  data['genre'] in tags['genre_x']: return "video"
        if data['genre'] in tags['genre_a']: return "audio"
        if data['genre'] in tags['genre_b']: return "book"
        
    if 'quality' in data:
        # it seeks in qualities
        if data['quality'] in tags['quality_v']: return "video"
        if data['quality'] in tags['quality_a']: return "audio"
    
    #torrent by default
    return "torrent"

def is_synonyms( s1, s2):
    if "other" in [s1,s2]:
        return True
    if s1 in synonyms:
        return s2 in synonyms[s1]
    if s2 in synonyms:
        return s1 in synonyms[s2]
    

def get_category_from_genre(s):
    s = s.lower().strip()
    if "|" in s:
        s = s.split("|")[0].strip()
    cat = None
    if s in tags['genre_v']:
        cat = "movie"
    if s in tags['genre_a']:
        cat = "music"
    if s in tags['genre_b']:
        cat = "book"
    if s in tags['genre_x']:
        cat = "adult"

    return cat
    
def get_category_from_subcategory(s):
    s = s.lower()
    if s in subcategory_category:
        return subcategory_category[s]
    else:
        return None

def is_category(s):
    if s is None: return False
    #if is large not is category
    
    if len(s.split()) > 2: return False
    
    return any([" %s "%cat in " %s "%s.lower() for cat in tags['category']])
        

def extract_category(s):
    cats = []
    for cat in tags['category']:
        if cat in s:
            return cat
            
    #~ return ",".join(list(set(cats))) if len(cats) > 0 else None
    
def is_language(s):
    if s is None: return False
    #if is large not is language
    if len(s.split()) > 2: return False
    
    return any([" %s "%l in " %s "%s.lower() for l in tags['language']])

def is_quality(s):
    if s is None: return False
    #if is large not is quality
    if len(s.split()) > 2: return False
    
    return any([" %s "%q in " %s "%s.lower() for q in tags['quality_v'] + tags['quality_a']])

def is_genre(s):
    if s is None: return False
    #if is large not is language genre
    if len(s.split()) > 4: return False
    return any([" %s "%g in " %s "%s.lower() for g in tags['genre_v'] + tags['genre_a'] + tags['genre_b'] + tags['genre_x']])


def is_season_episode(s):
    se = re.findall("[s|S]([0-9]+)[e|E]([0-9]+)",s)
    if se:
        return {'s':se[0][0], 'e':se[0][1]}


def is_tag(s):
    for tag, keywords in tags.items():
        if any([" %s "%keyword == " %s "%s.lower() for keyword in keywords]):
            return tag
    return False
    
def extract_keywords(s):
    return ",".join([w for w in s.lower().split() if len(w) > 2 and " %s " % w in ",".join([" , ".join(v) for v in tags.values()])])
    

def is_script(s, soft = False):
    #try to determine if is code
    s = s.lower().replace(" ", "")
    
    blacklist = ["text/javascript", "function", "<iframe", "text-decoration", "font-size", "=[", "= [", "={", "= {", "return false", "return true"]
    if not soft:
        blacklist.extend([ "src=", "href="])
    if any([w in s for w in blacklist]):
        return True
    
    if soft:
        return (s.count(";\n") + s.count("}\n") + s.count("{\n") + s.count("+\n") ) > (s.count("\n")/2)
    else:
        return (s.count(";\n") + s.count("}\n") + s.count("{\n") + s.count("+\n") + s.count(">\n") + s.count(",\n"))> (s.count("\n")/2)


def is_size(s):
    #number and unit of measure
    if not isinstance(s, basestring):
        return s
    
    s = s.lower()
    if any([w in s for w in ['size','gb','mb','kb']]):
        s = s.replace("gb", " gb").replace("mb", " mb").replace("kb", " kb")
        p = re.compile('\d+(\.\d+)?')
        try:
            ns = [float(w) for w in s.split() if p.match(w)]
        except ValueError:
            return False
        if len(ns) == 1:
            
            z = float(ns[0])
            _z = z
            
            if "kb" in s: z *= 1024
            if "mb" in s: z *= (1024 ** 2)
            if "gb" in s: z *= (1024 ** 3)
            if _z == z and "." in s:
                #mb
                z *= (1024 ** 2)
                
            return int(z)
    return False

def is_title(s, url, full = False):
    #the words in the url
    if s is None:
        return False
    
    if not full:
        url = url.split("/")[-1]
    else:
        #without domain
        url = "/".join(url.split("/")[3:])
    
    words = re.findall(r"[\w']+", s.lower())
    
    matches = 0.0
    for w in words:
        if w in url.lower():
            matches += 1
    count_words = float(len(words))
    
    
    return matches == count_words and count_words > 0
    #~ return matches > 1 and (count_words * ((count_words-1)/count_words)) <= matches


def is_description(s):
    blacklist = ["/announce", "add your comment", 'id="comment', 'class="comment', "id='comment", "class='comment"]
    return not any(w in s for w in blacklist) and len(s) < 4096 and len(s) > 32 and not is_script(s, soft = True) and not(len(s) < 50 and "thank" in s) 


def extract_infohash(s):
    rt = re.findall("[0-9a-fA-F]{40}",s)
    if len(rt)>0:
        return rt[0].upper()
    else:
        return None

        
def is_valid_meta(s, name):
    # * infohash
    # * size
    # * description
    # * title
    # * tags
    # * quality
    # * genre
    # * season
    # * episode
    # * language
    
    if isinstance(s, basestring):
        s = s.lower()
    
    
    rt = False
    if name == "infohash":
        return extract_infohash(s)
    if name == "size":
        return is_size(s)
    if name == "description":
        if not is_description(s):
            return False
        else:
            return True
        #~ return is_description(s)
    if name == "title":
        #No hay datos
        return True
    if name == "tags":
        return is_tag(s)
    if name == "quality":
        rt = is_quality(s)
    if name == "genre":
        rt = is_genre(s)
    if name in ["season", "episode"]:
        if s.isdigit():
            return True
            
        se = is_season_episode(s)
        if se:
            if name == "season":
                return se['s']
            if name == "episode":
                return se['e']
        else:
            return False
    if name == "language" or name == "languages":
        rt = is_language(s)
    if name == "image":
        return s.count("/") > 2
    if name == "category" or name == "categories":
        if is_category(s):
            rt = extract_category(s)
    
    
    if rt:
        return rt
    else:
        t = is_tag(s)
        if is_tag(s):
            return t
        else:
            return False
    
    
