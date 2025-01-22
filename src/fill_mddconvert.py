import argparse
import pandas as pd
import sys, os
#import xlsxwriter
import time
from pathlib import Path
from datetime import datetime
#from multiprocessing import Process
import re



#import pdb



def fill(df):
    print("\nFilling MDD_Convert...")
    
    rows = df.index
    row = None
    print('   starting working on MDD_Convert sheet...')
    all_names = []
    for row in rows:
        all_names.append('{c}'.format(c=df.loc[row,'Variable Name']))
    all_names = ' '.join(all_names)
    print('   iterating over rows in MDD_Convert sheet...')
    report = []
    global_dict_labels = dict()
    for row in rows:
        spss_name = df.loc[row,'Variable Name']
        processing_last_item = spss_name
        try:
            report.append('spss variable {var}'.format(var=spss_name))
            report.append('reading...')
            map_type = df.loc[row,'Question Type']
            spss_label = df.loc[row,'Variable Label']
            map_catname = df.loc[row,'Category Name']
            map_catlabel = df.loc[row,'Category Label']
            map_categories = df.loc[row,'Variable Categories']
            map_categories = map_categories = dict(p for p in re.findall(r'\|(\d+)\|([^\|]+)','{add}{syntax}'.format(add='|',syntax=map_categories),flags = re.DOTALL|re.ASCII))
            # some rowes are incorrectly identified as "Cat Flags" (multi-punch) while they clearly have more response options than 1; I'll just convert it to "Cat Single" (single-punch)
            if (len(set(map_categories.keys())-{'1','0'}) > 0) and (map_type=='Cat Flags'):
                map_type = 'Cat Single'
            map_notes = df.loc[row,'Question Note']
            map_notes = re.sub(r'\s*;\s*',';',map_notes)
            map_notes = map_notes.split(';')
            map_notes = [ part for part in map_notes if re.match(r'[^\s]',part) ]
            map_temp_groupsidentifiedbypainlessprogram = df.loc[row,'Variable Groups'].split('|')
            # prep variable that stores all results from parsing - iterator names, labels, other parsed parts...
            spss_name_parts = {'name_orig':spss_name}
            # start parsing
            spss_name_sanitized = re.sub(r'((?:U|u)ser)(\d+)',lambda parts:'u{d}'.format(d=parts[2]),spss_name,flags = re.DOTALL|re.ASCII)
            re_gridresults = None
            re_gridresults_pattern = None
            re_gridresults_searchsimilar_fn = lambda namebase,l1,l2,namesuffix: r'^$'
            # try all patterns
            if not re_gridresults:
                # 1. r1c1 pattern
                re_gridresults = re.match(r'^\s*?(\w+?)_?r(\d+)(?:_?(?:c|r)(\d+))?((?:(?:(?:_\w+)|(?:oe)))?)$',spss_name_sanitized,flags = re.DOTALL|re.ASCII)
                if re_gridresults:
                    re_gridresults_pattern = 'r1c1'
                    re_gridresults_searchsimilar_fn = lambda namebase,l1,l2,namesuffix: r'\s*?'+namebase+r'_?r'+l1+r'(?:_?(?:c|r)'+l2+r')?'+namesuffix+r''
            if not re_gridresults:
                # 2. _1_1 pattern
                re_gridresults = re.match(r'^\s*?(\w+?)_(\d+)(?:_(\d+))?((?:(?:(?:_\w+)|(?:oe)))?)$',spss_name_sanitized,flags = re.DOTALL|re.ASCII)
                if re_gridresults:
                    re_gridresults_pattern = '_1_1'
                    re_gridresults_searchsimilar_fn = lambda namebase,l1,l2,namesuffix: r'\s*?'+namebase+r'_'+l1+r'(?:_'+l2+r')?'+namesuffix+r''
            #if not re_gridresults:
            #    re_gridresults = re.match(r'^\s*?(\w+?)_?(\d{2}\d*)(?:c(\d+))?((?:(?:(?:_\w+)|(?:oe)))?)$',spss_name_sanitized,flags = re.DOTALL|re.ASCII)
            #if there is a pattern matching grid
            if re_gridresults:
                spss_name_parts['name_base'] = '{d}'.format(d=re_gridresults[1])
                spss_name_parts['name_suffix'] = '{d}'.format(d=re_gridresults[4])
                spss_name_parts['notes'] = []
                spss_name_parts['num_similar'] = len(re.findall(r'\b'+re_gridresults_searchsimilar_fn(namebase=spss_name_parts['name_base'],l1=r'\d+',l2=r'\d*',namesuffix=spss_name_parts['name_suffix'])+r'\b',all_names,flags = re.DOTALL|re.ASCII))
                spss_name_parts['l1_iter'] = 'r{d}'.format(d=re_gridresults[2]) if not not re_gridresults[2] else None
                spss_name_parts['l2_iter'] = 'c{d}'.format(d=re_gridresults[3]) if not not re_gridresults[3] else None
                # now read labels
                # and parse it (split into parts that mean question name, iter label, and overall question label)
                # the pattern we expect is: "qname: iter label - overall label"
                # # TODO: piece of shit
                # re_label_results = re.match(r'^\s*?(?:(\w+)\s*?\:\s*)?((?:(?:.*?) - )*)(.*?)\s*?$',spss_label)
                # spss_name_parts['label_qname'] = re_label_results[1]
                # spss_name_parts['label_iter'] = re_label_results[2].split(' - ')
                # spss_name_parts['label_main'] = re_label_results[3]
                # alternative approach - we find all labels for all questions that match the same pattern, and find the part that stays always the same, and find the part that changes
                #pdb.set_trace()
                spss_name_parts['label_main'] = spss_label
                spss_name_parts['label_qname'] = ''
                spss_name_parts['label_l1'] = None
                spss_name_parts['label_l2'] = None
                q_cmp_label_methods = ['basepart','l1','l2']
                if not spss_name_parts['l2_iter']:
                    q_cmp_label_methods = ['basepart','l1']
                if not spss_name_parts['l1_iter']:
                    q_cmp_label_methods = ['basepart']
                #let's go - iteate over all rows
                for q_cmp_label_method in q_cmp_label_methods:
                    # let's first check if we can pull cached results; it is cached in global_dict_labels var
                    q_cmp_labels_canproceedfromdict = False
                    if spss_name in global_dict_labels.keys():
                        #found cached value - no need to iterate over all rows in Excel again
                        if q_cmp_label_method=='basepart':
                            if not ('label_main' in global_dict_labels[spss_name].keys()):
                                q_cmp_labels_canproceedfromdict = False
                            else:
                                q_cmp_labels_canproceedfromdict = True
                                spss_name_parts['label_main'] = global_dict_labels[spss_name]['label_main']
                                spss_name_parts['label_qname'] = global_dict_labels[spss_name]['label_qname']
                        if q_cmp_label_method=='l1':
                            if not ('label_l1' in global_dict_labels[spss_name].keys()):
                                # print('Error: {var}: label dictionary: missing l1'.format(var=spss_name))
                                # #raise Exception('label dictionary: missing l1')
                                q_cmp_labels_canproceedfromdict = False
                            else:
                                q_cmp_labels_canproceedfromdict = True
                                spss_name_parts['label_l1'] = global_dict_labels[spss_name]['label_l1']
                        if q_cmp_label_method=='l2':
                            if not ('label_l2' in global_dict_labels[spss_name].keys()):
                                # print('Error: {var}: label dictionary: missing l2'.format(var=spss_name))
                                # #raise Exception('label dictionary: missing l2')
                                q_cmp_labels_canproceedfromdict = False
                            else:
                                q_cmp_labels_canproceedfromdict = True
                                spss_name_parts['label_l2'] = global_dict_labels[spss_name]['label_l2']
                    # ok, no cached results, let's do our work and iterate over labels and compare it
                    if not q_cmp_labels_canproceedfromdict:
                        # let's prep search expression that we can use to find similar rows - it depends if we are iterating over l1, l2, or neigher
                        q_cmp_re_searsimilarpattern = r'\b'+re_gridresults_searchsimilar_fn(namebase=spss_name_parts['name_base'],l1=(spss_name_parts['l1_iter'] if q_cmp_label_method=='l1' else r'\d+'),l2=(spss_name_parts['l2_iter'] if q_cmp_label_method=='l2' else r'\d*'),namesuffix=spss_name_parts['name_suffix'])+r'\b'
                        if q_cmp_label_method=='basepart':
                            q_cmp_re_searsimilarpattern = r'\b'+re_gridresults_searchsimilar_fn(namebase=spss_name_parts['name_base'],l1=r'\d+',l2=r'\d*',namesuffix=spss_name_parts['name_suffix'])+r'\b'
                        elif q_cmp_label_method=='l1':
                            # iterate over l1, fix l2 part the same as it was matching in re_gridresults (even if it's blank (no l2) - we'll fix it blank)
                            q_cmp_re_searsimilarpattern = r'\b'+re_gridresults_searchsimilar_fn(namebase=spss_name_parts['name_base'],l1=r'\d+',l2=(re_gridresults[3] if re_gridresults[3] else ''),namesuffix=spss_name_parts['name_suffix'])+r'\b'
                        elif q_cmp_label_method=='l2':
                            # iterate over l2, fix l1 part the same as it was matching in re_gridresults
                            q_cmp_re_searsimilarpattern = r'\b'+re_gridresults_searchsimilar_fn(namebase=spss_name_parts['name_base'],l1=(re_gridresults[2] if re_gridresults[2] else ''),l2=r'\d+',namesuffix=spss_name_parts['name_suffix'])+r'\b'
                        q_cmp_labelsall = []
                        for row_cmp in rows:
                            q_cmp_varname = df.loc[row_cmp,'Variable Name']
                            # if q_cmp_label_method=='l1':
                            #     print('debug: for reference, l1_iter = {var}'.format(var=spss_name_parts['l1_iter']))
                            # if q_cmp_label_method=='l2':
                            #     print('debug: for reference, l2_iter = {var}'.format(var=spss_name_parts['l2_iter']))
                            q_cmp_ismatching = re.match(r'^'+q_cmp_re_searsimilarpattern+r'$',q_cmp_varname,flags = re.DOTALL|re.ASCII)
                            # print('debug: checking for matching labels, method = {method}, ref = {ref} ( row = {row_base} ), checking = {checking} ( row cmp = {row_cmp} ), base part = {basepart}, matching = {matching}'.format(ref=spss_name,checking=q_cmp_varname,row_base=row,row_cmp=row_cmp,matching=('true' if q_cmp_ismatching else 'false'),basepart=spss_name_parts['name_base'], method=q_cmp_label_method))
                            if q_cmp_ismatching:
                                q_cmp_label = df.loc[row_cmp,'Variable Label']
                                q_cmp_label_sanitized = re.sub(r'^\s*?('+q_cmp_re_searsimilarpattern+r')((?:\s*\:\s*)|(?:\s*-\s*)|(?:\.\s*))'+r'(.*?)$',lambda m: '{part1}{part2}{part3}'.format(part1='',part2='',part3=m[3]),q_cmp_label,flags = re.DOTALL|re.ASCII)
                                # print('debug: adding {l} to the list'.format(l=q_cmp_label))
                                q_cmp_labelsall.append(q_cmp_label_sanitized)
                                # print('debug: q_cmp_labelsall = {var}'.format(var=q_cmp_labelsall))
                            # else:
                            #     print('debug: for reference, q_cmp_labelsall = {var}'.format(var=q_cmp_labelsall))
                        # print('debug: q_cmp_labelsall = {var}'.format(var=q_cmp_labelsall))
                        if( len(q_cmp_labelsall)==0 ):
                            raise ValueError('trying to find all labels - something is going wrong, method = {m}, list = {llen}: {items}'.format(m=q_cmp_label_method,llen=len(q_cmp_labelsall),items=q_cmp_labelsall))
                        if( ((len(q_cmp_labelsall)==1) and (q_cmp_label_method=='l1')) or ((len(q_cmp_labelsall)==1) and (q_cmp_label_method=='l2')) ):
                            #raise ValueError('trying to find all labels - something is probably going wrong, method = {m}, list = {llen}: {items}'.format(m=q_cmp_label_method,llen=len(q_cmp_labelsall),items=q_cmp_labelsall))
                            # it's not a global error but that;s an issue for us
                            # there is only one row matching the patter - we can't compare labels, we can't compare one row against itself - it does not make sense
                            pass
                        if( len(q_cmp_labelsall)==1 ):
                            q_cmp_labelsref = q_cmp_labelsall[0]
                            if q_cmp_label_method == 'basepart':
                                spss_name_parts['label_main'] = '{part1} {part2}'.format(part1=q_cmp_labelsref,part2='')
                                spss_name_parts['label_qname'] = ''
                                if re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',spss_name_parts['label_main']):
                                    matches = re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',spss_name_parts['label_main'])
                                    spss_name_parts['label_qname'] = matches[1]
                                    spss_name_parts['label_main'] = matches[2]
                                spss_name_parts['label_main'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),spss_name_parts['label_main'])
                            elif q_cmp_label_method == 'l1':
                                spss_name_parts['label_l1'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_labelsref)
                            elif q_cmp_label_method == 'l2':
                                spss_name_parts['label_l2'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_labelsref)
                        else:
                            q_cmp_labelsref = q_cmp_labelsall[0]
                            q_cmp_label_minlen = len(q_cmp_labelsref)
                            for l in q_cmp_labelsall:
                                q_cmp_label_minlen = len(l) if len(l) < q_cmp_label_minlen else q_cmp_label_minlen
                            q_cmp_labelspart_allthesame = False
                            q_cmp_label_matchingpartbeginlen = q_cmp_label_minlen
                            while (not q_cmp_labelspart_allthesame) and (q_cmp_label_matchingpartbeginlen>0):
                                ffound = True
                                for l in q_cmp_labelsall:
                                    ffound = ffound and (l[:q_cmp_label_matchingpartbeginlen]==q_cmp_labelsref[:q_cmp_label_matchingpartbeginlen])
                                if ffound:
                                    q_cmp_labelspart_allthesame = True
                                else:
                                    q_cmp_label_matchingpartbeginlen = q_cmp_label_matchingpartbeginlen - 1
                            q_cmp_labelsref = q_cmp_labelsall[0]
                            q_cmp_labelspart_allthesame = False
                            q_cmp_label_matchingpartendlen = q_cmp_label_minlen
                            while (not q_cmp_labelspart_allthesame) and (q_cmp_label_matchingpartendlen>0):
                                ffound = True
                                for l in q_cmp_labelsall:
                                    ffound = ffound and (l[( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None ):]==q_cmp_labelsref[( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None ):])
                                if ffound:
                                    q_cmp_labelspart_allthesame = True
                                else:
                                    q_cmp_label_matchingpartendlen = q_cmp_label_matchingpartendlen - 1
                            if q_cmp_label_method == 'basepart':
                                spss_name_parts['label_main'] = '{part1} {part2}'.format(part1=q_cmp_labelsref[:q_cmp_label_matchingpartbeginlen],part2=(q_cmp_labelsref[-q_cmp_label_matchingpartendlen:] if q_cmp_label_matchingpartendlen>0 else ''))
                                spss_name_parts['label_qname'] = ''
                                if re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',spss_name_parts['label_main']):
                                    matches = re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',spss_name_parts['label_main'])
                                    spss_name_parts['label_qname'] = matches[1]
                                    spss_name_parts['label_main'] = matches[2]
                                spss_name_parts['label_main'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),spss_name_parts['label_main'])
                                #print('debug: var = {var}, updating label to "{part2}"'.format(var=spss_name,part2=spss_name_parts['label_main']))
                                # and store it to global cache
                                #if not (spss_name in global_dict_labels.keys()):
                                #    global_dict_labels[spss_name] = dict()
                                #global_dict_labels[spss_name]['label_main'] = spss_name_parts['label_main']
                                #global_dict_labels[spss_name]['label_qname'] = spss_name_parts['label_qname']
                            elif q_cmp_label_method == 'l1':
                                spss_name_parts['label_l1'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_labelsref[q_cmp_label_matchingpartbeginlen:( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None )])
                                #print('debug: var = {var}, updating l1 label to "{part1}"'.format(var=spss_name,part1=spss_name_parts['label_l1']))
                                #global_dict_labels[spss_name]['label_l1'] = spss_name_parts['label_l1']
                            elif q_cmp_label_method == 'l2':
                                spss_name_parts['label_l2'] = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_labelsref[q_cmp_label_matchingpartbeginlen:( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None )])
                                #print('debug: var = {var}, updating l2 label to "{part1}"'.format(var=spss_name,part1=spss_name_parts['label_l2']))
                                #global_dict_labels[spss_name]['label_l2'] = spss_name_parts['label_l2']
                            for row_update in rows:
                                spss_name_update = df.loc[row_update,'Variable Name']
                                if re.match(q_cmp_re_searsimilarpattern,spss_name_update,flags = re.DOTALL|re.ASCII):
                                    q_cmp_label = df.loc[row_update,'Variable Label']
                                    q_cmp_label_sanitized = re.sub(r'^\s*?('+q_cmp_re_searsimilarpattern+r')((?:\s*\:\s*)|(?:\s*-\s*)|(?:\.\s*))'+r'(.*?)$',lambda m: '{part1}{part2}{part3}'.format(part1='',part2='',part3=m[3]),q_cmp_label,flags = re.DOTALL|re.ASCII)
                                    if not (spss_name_update in global_dict_labels.keys()):
                                        global_dict_labels[spss_name_update] = dict()
                                    global_dict_labels[spss_name_update]['updated_as'] = spss_name # for debugging purposes
                                    global_dict_labels[spss_name_update]['updated_processing_methods'] = ','.join(q_cmp_label_methods) # for debugging purposes
                                    if q_cmp_label_method == 'basepart':
                                        val_label_main = '{part1} {part2}'.format(part1=q_cmp_label_sanitized[:q_cmp_label_matchingpartbeginlen],part2=(q_cmp_label_sanitized[-q_cmp_label_matchingpartendlen:] if q_cmp_label_matchingpartendlen>0 else ''))
                                        val_label_qname = ''
                                        if re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',val_label_main):
                                            matches = re.match(r'^\s*?(\w+)\s*?\:\s*?([^\s].*?)\s*?$',val_label_main)
                                            val_label_qname = matches[1]
                                            val_label_main = matches[2]
                                        val_label_main = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),val_label_main)
                                        # and store it to global cache
                                        global_dict_labels[spss_name_update]['label_main'] = val_label_main
                                        global_dict_labels[spss_name_update]['label_qname'] = val_label_qname
                                        #print('debug: also updating: var = {var}, updating label to "{part1}"'.format(var=spss_name_update,part1=global_dict_labels[spss_name_update]['label_main']))
                                    elif q_cmp_label_method == 'l1':
                                        val_label_l1 = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_label_sanitized[q_cmp_label_matchingpartbeginlen:( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None )])
                                        global_dict_labels[spss_name_update]['label_l1'] = val_label_l1
                                        #print('debug: also updating: var = {var}, updating l1 label to "{part1}"'.format(var=spss_name_update,part1=global_dict_labels[spss_name_update]['label_l1']))
                                    elif q_cmp_label_method == 'l2':
                                        val_label_l2 = re.sub(r'^\s*(.*?)\s*$',lambda m:'{content}'.format(content=m[1]),q_cmp_label_sanitized[q_cmp_label_matchingpartbeginlen:( -q_cmp_label_matchingpartendlen if q_cmp_label_matchingpartendlen>0 else None )])
                                        global_dict_labels[spss_name_update]['label_l2'] = val_label_l2
                                        #print('debug: also updating: var = {var}, updating l2 label to "{part1}"'.format(var=spss_name_update,part1=global_dict_labels[spss_name_update]['label_l2']))

                # check if we don't need to treat it as a loop/grid - sanitize a lil bit
                if spss_name_parts['l1_iter']:
                    #report.append('"spss_name_parts[\'num_similar\']<=1 and len(spss_name_parts[\'label_iter\']) == 0" == {f}'.format(f='true' if spss_name_parts['num_similar']<=1 and len([part for part in spss_name_parts['label_iter'] if re.match(r'[^\s]',part)]) == 0 else 'false'))
                    #if spss_name_parts['num_similar']<=1 and len([part for part in spss_name_parts['label_iter'] if re.match(r'[^\s]',part)]) == 0:
                    report.append('"spss_name_parts[\'num_similar\']<=1 and not(spss_name_parts[\'label_l1\'])" == {f}'.format(f='true' if spss_name_parts['num_similar']<=1 and not(spss_name_parts['label_l1']) else 'false'))
                    if spss_name_parts['num_similar']<=1 and not(spss_name_parts['label_l1']):
                        spss_name_parts['l1_iter'] = None
                        spss_name_parts['l2_iter'] = None
                # # now we need to write l1 iter label
                # if spss_name_parts['l1_iter']:
                #     if len(spss_name_parts['label_iter'])>0 and not not spss_name_parts['label_iter'][0] and len(re.sub(r'^\s*(.*?)\s*$',lambda parts: '{m}'.format(m=parts[1]),spss_name_parts['label_iter'][0]))>0:
                #         # if it exists, we write the label
                #         spss_name_parts['l1_iter_label'] = re.sub(r'^\s*(.*?)\s*$',lambda parts: '{m}'.format(m=parts[1]),spss_name_parts['label_iter'][0])
                #     else:
                #         # if not, we write iter name (category name)
                #         warn_msg = 'WARNIG: could not read iter label part, spss_variable_name = {s1}, iter = level 1 (we expect it to follow this format: var_name: label level 1 - label overall)'.format(s1=spss_name)
                #         spss_name_parts['notes'].append(warn_msg)
                #         print(warn_msg)
                #         spss_name_parts['l1_iter_label'] = '{v}: {p}'.format(v=spss_name_parts['l1_iter'],p=spss_name_parts['label_main'])
                # # now same for l2
                # if spss_name_parts['l2_iter']:
                #     spss_name_parts['l2_iter_label'] = spss_name_parts['l1_iter_label']
                #     if len(spss_name_parts['label_iter'])>1 and not not spss_name_parts['label_iter'][1] and len(re.sub(r'^\s*(.*?)\s*$',lambda parts: '{m}'.format(m=parts[1]),spss_name_parts['label_iter'][1]))>0:
                #         spss_name_parts['l1_iter_label'] = re.sub(r'^\s*(.*?)\s*$',lambda parts: '{m}'.format(m=parts[1]),spss_name_parts['label_iter'][1])
                #     else:
                #         warn_msg = 'WARNIG: could not read iter label part, spss_variable_name = {s1}, iter = level 2 (we expect it to follow this format: var_name: label level 1 - label level 2 - label overall)'.format(s1=spss_name)
                #         spss_name_parts['notes'].append(warn_msg)
                #         print(warn_msg)
                #         spss_name_parts['l1_iter_label'] = '{v}: {p}'.format(v=spss_name_parts['l2_iter'],p=spss_name_parts['label_main'])
                # now we need to write l1 iter label
                if spss_name_parts['l1_iter']:
                    if len(spss_name_parts['label_l1'])>0:
                        # if it exists, we write the label
                        spss_name_parts['l1_iter_label'] = spss_name_parts['label_l1']
                    else:
                        # if not, we write iter name (category name)
                        warn_msg = 'WARNIG: could not read iter label part, spss_variable_name = {s1}, iter = level 1 (no unique part found that is different from line to line)'.format(s1=spss_name)
                        spss_name_parts['notes'].append(warn_msg)
                        print(warn_msg)
                        spss_name_parts['l1_iter_label'] = '{v}: {p}'.format(v=spss_name_parts['l1_iter'],p=spss_name_parts['label_main'])
                # now same for l2
                if spss_name_parts['l2_iter']:
                    if len(spss_name_parts['label_l2'])>0:
                        spss_name_parts['l2_iter_label'] = spss_name_parts['label_l2']
                    else:
                        warn_msg = 'WARNIG: could not read iter label part, spss_variable_name = {s1}, iter = level 2 (no unique part found that is different from line to line)'.format(s1=spss_name)
                        spss_name_parts['notes'].append(warn_msg)
                        print(warn_msg)
                        spss_name_parts['l2_iter_label'] = '{v}: {p}'.format(v=spss_name_parts['l2_iter'],p=spss_name_parts['label_main'])
                # store warnings: we store it as an array, not string, so that we can exclude duplicate warnings
                for warn_msg in spss_name_parts['notes']:
                    if warn_msg in map_notes:
                        pass
                    else:
                        map_notes.append(warn_msg)
            report.append('parse results: {s}'.format(s=spss_name_parts))
            report.append('generating codes for autofill...')
            # now we prepare variables that store final values that will be written to Excel
            col_QName = spss_name
            col_QLabel = spss_label
            col_QType = map_type
            col_CatName = map_catname
            col_CatLabel = map_catlabel
            col_QNotes = ' ; '.join(map_notes)
            col_LoopName = ""
            col_LoopLabel = ""
            col_IterName = ""
            col_IterLabel = ""
            col_LoopL2Name = ""
            col_LoopL2Label = ""
            col_IterL2Name = ""
            col_IterL2Label = ""
            if 'l1_iter' in spss_name_parts:
                col_QName = '{p1}{p2}'.format(p1=spss_name_parts['name_base'],p2=spss_name_parts['name_suffix'])
                col_QLabel = '{p1}: {p2}'.format(p1=col_QName,p2=spss_name_parts['label_main'])
            # now we sanitize question names - if it starts with "h" or "hid", we add a "DV_"
            # refer to map_temp_groupsidentifiedbypainlessprogram
            for prefix in ['DV_','hid','h','_']:
                if re.match(r'^\s*?('+prefix+r')(\w+)\s*?$',col_QName,flags = re.DOTALL|re.ASCII) and prefix in map_temp_groupsidentifiedbypainlessprogram:
                    col_QName = re.sub(r'^\s*?('+prefix+r')(\w+)\s*?$',lambda parts: 'DV_{p}'.format(p=parts[2]),col_QName,flags = re.DOTALL|re.ASCII)
            col_QName = re.sub(r'^\s*?(date)\s*?$','DV_date',col_QName,flags = re.DOTALL|re.ASCII|re.I)
            col_QName = re.sub(r'[^\w]','_',col_QName,flags = re.DOTALL|re.ASCII)
            col_QName = re.sub(r'^_*(\w+?)_*$',lambda parts: '{p}'.format(p=parts[1]),col_QName,flags = re.DOTALL|re.ASCII)
            # continue preparing l1 and l2 iter names to write out - stored in col_LoopName, col_LoopLabel, col_IterName, col_IterLabel, col_LoopL2Name, col_... and so on
            q_isL1Loop = ('l1_iter' in spss_name_parts) and spss_name_parts['l1_iter']
            q_isL2Loop = ('l2_iter' in spss_name_parts) and spss_name_parts['l2_iter']
            if q_isL1Loop:
                if not q_isL2Loop:
                    if map_type=='Cat Flags':
                        # skip - that's a multi-punch, that's it, we are good to go
                        col_CatName = spss_name_parts['l1_iter']
                        col_CatLabel = spss_name_parts['l1_iter_label']
                        pass
                    else:
                        # a loop or grid
                        col_LoopName = col_QName
                        col_LoopLabel = col_QLabel
                        col_IterName = spss_name_parts['l1_iter']
                        col_IterLabel = spss_name_parts['l1_iter_label']
                else:
                    if map_type=='Cat Flags':
                        # a loop or grid
                        col_LoopName = col_QName
                        col_LoopLabel = col_QLabel
                        col_IterName = spss_name_parts['l1_iter']
                        col_IterLabel = spss_name_parts['l1_iter_label']
                        col_CatName = spss_name_parts['l2_iter']
                        col_CatLabel = spss_name_parts['l2_iter_label']
                    else:
                        # 2-level loop
                        #raise ValueError('2-level loop')
                        col_LoopName = col_QName
                        col_LoopLabel = col_QLabel
                        col_IterName = spss_name_parts['l1_iter']
                        col_IterLabel = spss_name_parts['l1_iter_label']
                        col_LoopL2Name = col_LoopName
                        col_LoopL2Label = col_LoopLabel
                        col_LoopName = 'GV'
                        col_LoopLabel = col_QName
                        col_IterL2Name = col_IterName
                        col_IterL2Label = col_IterLabel
                        col_IterName = spss_name_parts['l2_iter']
                        col_IterLabel = spss_name_parts['l2_iter_label']
            # go on and write!
            df.loc[row,'Question Name'] = col_QName
            df.loc[row,'Question Label'] = col_QLabel
            df.loc[row,'Question Type'] = col_QType
            df.loc[row,'Question Note'] = col_QNotes
            df.loc[row,'Category Name'] = col_CatName
            df.loc[row,'Category Label'] = col_CatLabel
            df.loc[row,'Loop L1 Name'] = col_LoopName
            df.loc[row,'Loop L1 Label'] = col_LoopLabel
            df.loc[row,'Iter L1 Name'] = col_IterName
            df.loc[row,'Iter L1 Label'] = col_IterLabel
            df.loc[row,'Loop L2 Name'] = col_LoopL2Name
            df.loc[row,'Loop L2 Label'] = col_LoopL2Label
            df.loc[row,'Iter L2 Name'] = col_IterL2Name
            df.loc[row,'Iter L2 Label'] = col_IterL2Label
            # done!
        except Exception as e:
            # something failed? alert which was the last row, and throw the exception back
            print('Error at {s}'.format(s=processing_last_item))
            raise e


    print("\nFilling finished")
    return report

def main(args):

    print("\nStart Time of Program :", datetime.now().strftime("%H:%M:%S"), "\n")
    #print("\nWarning: the program will write to this file; keeping backups is up on you")
    #start_time = time.perf_counter()
    
    ## Set Variables
    indexColName = None # args.index
    path_Excel = Path(args.map)
    
    df = None
    print("\n"+'Reading Excel "{file}"...'.format(file=path_Excel))
    # header=7 means how many rows to skip above the banner line
    df = pd.read_excel(path_Excel, sheet_name='MDD_Convert', index_col='Index',header=7,engine='openpyxl').fillna("")
    print("\n"+'Reading Excel successful')
    
    report = fill(df)

    ### Complete the processes
    #for proc in procs:
    #    proc.join()
    
    print("\n"+'Done, saving...') 
    
    try:
        path_outputLog = '{path_base}{suffix}{ext}'.format(path_base=re.sub(r'^(.*?)((?:\.xl\w+)?)$',lambda parts: '{part1}'.format(part1=parts[1]),'{path_Excel}'.format(path_Excel=path_Excel)),suffix='_MDDConvert',ext='.log.txt')
        file_outputLog = open(path_outputLog, "w")
        file_outputLog.writelines(['{s}{linebreak}'.format(s=s,linebreak="\n") for s in report])
    except:
        pass

    ## Save output and format
    path_outputExcel = '{path_base}{suffix}{ext}'.format(path_base=re.sub(r'^(.*?)((?:\.xl\w+)?)$',lambda parts: '{part1}'.format(part1=parts[1]),'{path_Excel}'.format(path_Excel=path_Excel)),suffix='_MDDConvert',ext='.xlsx')
    print('Saving as {fname}...'.format(fname=path_outputExcel)) 
    df.to_excel(path_outputExcel, sheet_name='MDD_Convert')
    print("\n"+'Saved!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Pre-fill painless map"
    )
    parser.add_argument(
        '-m',
        '--map',
        metavar='mddmap.xlsx',
        help='path to excel file with mapping',
        required=True
    )
    args = parser.parse_args()
    main(args)