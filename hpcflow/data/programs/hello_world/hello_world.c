#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cJSON.h"

void hello_world()
{
    printf("hello, world\n");
}

void hello_world_ins_outs(const char *inputs_path, const char *outputs_path)
{
    printf("hello, world\n");

    // Read input JSON
    FILE *fp = fopen(inputs_path, "r");
    if (!fp)
    {
        perror("Failed to open input file");
        exit(1);
    }

    fseek(fp, 0, SEEK_END);
    long len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *data = malloc(len + 1);
    fread(data, 1, len, fp);
    data[len] = '\0';
    fclose(fp);

    cJSON *json = cJSON_Parse(data);
    if (!json)
    {
        fprintf(stderr, "Error parsing JSON input\n");
        free(data);
        exit(1);
    }

    double p1 = cJSON_GetObjectItem(json, "p1")->valuedouble;
    double p2 = cJSON_GetObjectItem(json, "p2")->valuedouble;
    double p3 = cJSON_GetObjectItem(json, "p3")->valuedouble;
    double p4 = p1 + p2 + p3;

    cJSON_Delete(json);
    free(data);

    // Create output JSON
    cJSON *output_json = cJSON_CreateObject();
    cJSON_AddNumberToObject(output_json, "p4", p4);

    char *out_string = cJSON_Print(output_json);

    fp = fopen(outputs_path, "w");
    if (!fp)
    {
        perror("Failed to open output file");
        cJSON_Delete(output_json);
        free(out_string);
        exit(1);
    }

    fprintf(fp, "%s\n", out_string);
    fclose(fp);

    cJSON_Delete(output_json);
    free(out_string);
}

int main(int argc, char *argv[])
{
    if (argc == 1)
    {
        hello_world();
    }
    else if (argc == 3)
    {
        hello_world_ins_outs(argv[1], argv[2]);
    }
    else
    {
        fprintf(stderr, "Usage: %s [input.json output.json]\n", argv[0]);
        return 1;
    }

    return 0;
}
