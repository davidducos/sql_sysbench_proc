/* 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)

*/

#include "sysbench_slow_proc.h"
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

void read_file_process();
void monitor_process();

guint num_threads= 4;
gchar *inputfile= NULL;
GMutex **mutex;
struct mapping *dir_mapping=NULL;

static GOptionEntry entries[] =
{
	{ "threads", 't', 0, G_OPTION_ARG_INT, &num_threads, "Number of threads to use, default 4", NULL },
        { "file", 'f', 0, G_OPTION_ARG_STRING, &inputfile, "Slow query log file", NULL },
	{ NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

int main(int argc, char *argv[]) {
        GError *error = NULL;
        GOptionContext *context;
  //      g_thread_init(NULL);

        context = g_option_context_new("multi-threaded MySQL dumping");
        GOptionGroup *main_group= g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
        g_option_group_add_entries(main_group, entries);
	g_option_context_set_main_group(context, main_group);
        if (!g_option_context_parse(context, &argc, &argv, &error)) {
                g_print ("option parsing failed: %s, try --help\n", error->message);
                exit (EXIT_FAILURE);
        }
        g_option_context_free(context);

	GThread **threads= g_new(GThread*, 2);
	threads[0]=g_thread_new("read_file_process",(GThreadFunc)read_file_process, NULL);
	threads[1]=g_thread_new("read_file_process",(GThreadFunc)monitor_process, NULL);
	mutex = g_new(GMutex*, num_threads);
	g_thread_join(threads[0]);
	g_thread_join(threads[1]);


//        threads[0]= g_thread_create((GThreadFunc)read_file_process, NULL, TRUE, NULL);
}

void print_thread(struct threads *t) {
	printf("\nThread_id: %s\t| Start line: %d\t| End line: %d", t->thread_id, t->start_line, t->end_line);
}

gint compare_threads(gconstpointer a, gconstpointer b){
        const struct threads *ab = a;
        const struct threads *bb = b;
        if (ab->thread_id == bb->thread_id)
                return 0;
	if (ab->thread_id)
		if (bb->thread_id)
		        return strcmp(ab->thread_id, bb->thread_id);
		else
			return 1;
	else
		return -1;
}

gint after_start(gconstpointer a, gconstpointer b){
        const struct threads *ab = a;
        const struct threads *bb = b;
//	printf("\n%d   %d   %d   %d   ---- %d",ab->start_line,ab->end_line, bb->start_line, bb->end_line, !(( bb->start_line > ab->start_line && bb->start_line < ab->end_line ) || ( bb->end_line > ab->start_line && bb->end_line < ab->end_line )));
        return !(( bb->start_line > ab->start_line && bb->start_line < ab->end_line ) || ( bb->end_line > ab->start_line && bb->end_line < ab->end_line ));
}

gint compare_thread_id(gconstpointer a, gconstpointer b){
        const struct mapping *ab = a;
        const struct mapping *bb = b;
        if (ab->thread_id)
                if (bb->thread_id){
                        return strcmp(ab->thread_id, bb->thread_id);
                }else{
                        return 1;
		}
        else
                if (bb->thread_id){
			return 1;
		}else{
			if (ab->dirnumber){
				printf("\nLS");
                                system(g_strdup_printf("ls -l %s/*.sql", ab->dirnumber));
			}else
				printf("\nNull");
			return 0;
		}
}

gint compare_dirnumber(gconstpointer a, gconstpointer b){
        const struct mapping *ab = a;
        const struct mapping *bb = b;
        if (ab->thread_id == bb->thread_id)
                return 0;
        if (ab->dirnumber)
                if (bb->dirnumber)
                        return strcmp(ab->dirnumber, bb->dirnumber);
                else
                        return 1;
        else
                return -1;
}


gboolean read_line(FILE *file, GString *data, gboolean *eof) {
        const int buffersize=512;
        char buffer[buffersize];
        do {
                if (fgets(buffer, buffersize, file) == NULL) {
                        if (feof(file)) {
                                *eof= TRUE;
                                buffer[0]= '\0';
                        } else {
                        	return FALSE;
                	}
		}                
                if (buffer[0] != '\n')
                        g_string_append(data, buffer);
        } while ((buffer[strlen(buffer)-1] != '\n') && *eof == FALSE);

        return TRUE;
}

void read_file_process(){
	printf ("STARTINGGGGGG");
        FILE *infile;
	FILE *outfile=NULL;
        gboolean eof=FALSE;
	struct stat st = {0};
	int i=0;
	int line=0;
	char * thread_id=NULL;
	GSList *thread_list=NULL;
	GSList *mapping_list=NULL;
	for (;i<num_threads;i++ ){
		char str[5];
		struct mapping *m=g_new0(struct mapping,1);
		m->dirnumber = g_strdup_printf("%i", i);
		m->thread_id=NULL;
		mapping_list = g_slist_append (mapping_list,m);
		sprintf(str, "%d", i);
		if (stat(str, &st) == -1) {
		    mkdir(str, 0700);
		}
		system(g_strdup_printf("rm -f %i/*", i));
	}



        infile= g_fopen(inputfile , "r");
	GString *statement;

        while (!feof(infile) && !eof){
                GString *data=g_string_new("");
                read_line(infile,data,&eof);
		line++;
                if (data != NULL && data->str != NULL){
			if (data->str[0]== '#'){
				if (strlen(data->str)!=0){
				        struct threads *t=g_new0(struct threads,1);
					t->thread_id=thread_id;
					GSList *t2=g_slist_find_custom(thread_list,t, (GCompareFunc)compare_threads);
        				if (t2){
						((struct threads*)(t2->data))->end_line=line;
						g_free(t);
					}else{
						t->end_line=line;
						t->start_line=line;
						thread_list = g_slist_append (thread_list,t);
					}
        	                }
				if (!g_ascii_strncasecmp("# Thread_id:",data->str,12)){
					gchar ** threadline=g_strsplit(data->str, " ", 4);
	                                thread_id=g_strdup(threadline[2]);
					g_strfreev(threadline);
				}
                        }
                }
		g_string_free(data,TRUE);
        }
	fclose(infile);

	
//	g_slist_foreach(thread_list, (GFunc)print_thread, NULL);
	GSList * elem=g_slist_next(g_slist_copy(thread_list));
	struct threads *t;
	int maxt=0;
	while (elem){
		t = (struct threads *)(elem->data);
		int a=1;
		GSList * t2=g_slist_find_custom(thread_list,t, (GCompareFunc)after_start);
		while(t2){
			a++;
			t2=g_slist_find_custom(g_slist_next(t2),t, (GCompareFunc)after_start);	
		}
		print_thread((struct threads *)t);
        	
		printf("\n---%d",a);
		if (a>maxt)
			maxt=a;
		elem=g_slist_next(elem);
	}

	if (num_threads<maxt){
		printf("You need more threads, before start!!\n");
//		exit(1);
	}
        infile= g_fopen(inputfile, "r");
        statement=g_string_new("");
	line=0;

	struct threads* thread_instance=NULL;
	eof=FALSE;

	char *outfilename=g_strdup("trash.sql");
	outfile= g_fopen(outfilename, "a");

        while (!feof(infile) && !eof){
                GString *data=g_string_new("");
                read_line(infile,data,&eof);
                line++;
                if (data != NULL && data->str != NULL){
                        if (data->str[strlen(data->str)-1]== '\n'){
                                data->str[strlen(data->str)-1]=' ';
                        }
                        if (data->str[0]== '#'){
				if (thread_instance && thread_instance->end_line<=line){
                                        struct mapping *mm=g_new0(struct mapping,1);
                                        mm->thread_id=thread_id;
                                        GSList *t4=g_slist_find_custom(mapping_list,mm, (GCompareFunc)compare_thread_id);
                                        g_free(mm);
					if (t4)
                                        	((struct mapping *)(t4->data))->thread_id=NULL;
				}

                                if (strlen(statement->str)!=0){
                                        write(fileno(outfile), statement->str, strlen(statement->str));
                                        write(fileno(outfile), "\n", 1);
					g_string_free(statement,TRUE);
                                        statement=g_string_new("");
                                }

                                if (!g_ascii_strncasecmp("# Thread_id:",data->str,12)){
                                        gchar ** threadline=g_strsplit(data->str, " ", 4);
                                        thread_id=g_strdup(threadline[2]);
					g_strfreev(threadline);

					struct threads *t=g_new0(struct threads,1);
                                        t->thread_id=thread_id;
                                        GSList *t2=g_slist_find_custom(thread_list,t, (GCompareFunc)compare_threads);
					g_free(t);
                                        thread_instance= (struct threads*)(t2->data);

                                        struct mapping *m=g_new0(struct mapping,1);
                                        m->thread_id=thread_id;
                                        GSList *t5=g_slist_find_custom(mapping_list, m, (GCompareFunc)compare_thread_id);
					g_free(m);
					if (!t5){
						m=g_new0(struct mapping,1);
						m->thread_id=NULL;
						t5=g_slist_find_custom(mapping_list, m, (GCompareFunc)compare_thread_id);
						g_free(m);
						if (!t5){
							printf("You need more threads!!\n");
							exit(1);
						}
					}
					dir_mapping = (struct mapping *)(t5->data);
					if (dir_mapping->thread_id && strcmp(dir_mapping->thread_id,thread_id)){
						printf("Que pacho? %s   %s   %s",dir_mapping->thread_id,thread_id,dir_mapping->dirnumber);
						exit(1);
					}
					dir_mapping->thread_id=thread_id;
					if (outfile)
						fclose(outfile);
					free(outfilename);
//					printf("\n%s/%s.sql", dir_mapping->dirnumber,dir_mapping->thread_id);
					outfilename = g_strdup_printf("%s/%s.sql", dir_mapping->dirnumber,dir_mapping->thread_id);
					outfile= g_fopen(outfilename, "a");
                                }
                        }else{
                                if (data->str[strlen(data->str)-2]== ';'){
                                        if (strlen(statement->str)!=0){
                                                write(fileno(outfile), statement->str, strlen(statement->str));
                                                write(fileno(outfile), "\n", 1);
                                        }
					g_string_free(statement,TRUE);
                                        statement=g_string_new("");
                                }
                                g_string_append(statement,data->str);
                        }
                }
		g_string_free(data,TRUE);
        }
        fclose(infile);
}

void monitor_process(){
	//cp from buffer to file

}
