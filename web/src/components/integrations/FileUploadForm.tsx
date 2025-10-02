import { useState, useCallback } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import { Button } from "@/components/ui/button";
import {
    Form,
    FormControl,
    FormDescription,
    FormField,
    FormItem,
    FormLabel,
    FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { api } from "@/lib/api";
import { toast } from "@/hooks/use-toast";
import { Upload, File as FileIcon } from "lucide-react";

const fileUploadSchema = z.object({
    name: z.string().min(1, "Name is required"),
    format: z.enum(["csv", "json", "xml", "parquet", "excel"]),
    delimiter: z.string().optional(),
    encoding: z.string().default("utf-8"),
    hasHeader: z.boolean().default(true),
});

type FileUploadFormValues = z.infer<typeof fileUploadSchema>;

interface FileUploadFormProps {
    onSuccess: () => void;
}

export function FileUploadForm({ onSuccess }: FileUploadFormProps) {
    const [file, setFile] = useState<File | null>(null);
    const [isDragging, setIsDragging] = useState(false);

    const form = useForm<FileUploadFormValues>({
        resolver: zodResolver(fileUploadSchema),
        defaultValues: {
            name: "",
            format: "csv",
            delimiter: ",",
            encoding: "utf-8",
            hasHeader: true,
        },
    });

    const handleDrop = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragging(false);

        const droppedFile = e.dataTransfer.files[0];
        if (droppedFile) {
            setFile(droppedFile);
            if (!form.getValues("name")) {
                form.setValue("name", droppedFile.name);
            }
        }
    }, [form]);

    const handleDragOver = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragging(true);
    }, []);

    const handleDragLeave = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragging(false);
    }, []);

    const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
        const selectedFile = e.target.files?.[0];
        if (selectedFile) {
            setFile(selectedFile);
            if (!form.getValues("name")) {
                form.setValue("name", selectedFile.name);
            }
        }
    };

    const onSubmit = async (data: FileUploadFormValues) => {
        if (!file) {
            toast({
                title: "No File Selected",
                description: "Please select a file to upload",
                variant: "destructive",
            });
            return;
        }

        try {
            await api.createIntegration({
                name: data.name,
                type: "file",
                status: "connected",
                description: `${data.format.toUpperCase()} file (${(file.size / 1024).toFixed(2)} KB)`,
                config: {
                    ...data,
                    fileName: file.name,
                    fileSize: file.size,
                    uploadedAt: new Date().toISOString(),
                },
            });

            toast({
                title: "Integration Created",
                description: `${data.name} has been successfully added`,
            });

            onSuccess();
        } catch (error) {
            toast({
                title: "Error",
                description: "Failed to create integration",
                variant: "destructive",
            });
        }
    };

    return (
        <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
                <div
                    onDrop={handleDrop}
                    onDragOver={handleDragOver}
                    onDragLeave={handleDragLeave}
                    className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${isDragging
                            ? "border-primary bg-primary/5"
                            : "border-muted-foreground/25 hover:border-primary/50"
                        }`}
                >
                    {file ? (
                        <div className="flex items-center justify-center gap-3">
                            <FileIcon className="h-8 w-8 text-primary" />
                            <div className="text-left">
                                <p className="font-medium">{file.name}</p>
                                <p className="text-sm text-muted-foreground">
                                    {(file.size / 1024).toFixed(2)} KB
                                </p>
                            </div>
                        </div>
                    ) : (
                        <>
                            <Upload className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
                            <p className="text-sm text-muted-foreground mb-2">
                                Drag and drop your file here, or click to browse
                            </p>
                            <Input
                                type="file"
                                onChange={handleFileSelect}
                                className="hidden"
                                id="file-upload"
                                accept=".csv,.json,.xml,.parquet,.xlsx,.xls"
                            />
                            <label htmlFor="file-upload">
                                <Button type="button" variant="outline" asChild>
                                    <span>Browse Files</span>
                                </Button>
                            </label>
                        </>
                    )}
                </div>

                <FormField
                    control={form.control}
                    name="name"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Integration Name</FormLabel>
                            <FormControl>
                                <Input placeholder="users.csv" {...field} />
                            </FormControl>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <div className="grid grid-cols-2 gap-4">
                    <FormField
                        control={form.control}
                        name="format"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>File Format</FormLabel>
                                <Select onValueChange={field.onChange} defaultValue={field.value}>
                                    <FormControl>
                                        <SelectTrigger>
                                            <SelectValue />
                                        </SelectTrigger>
                                    </FormControl>
                                    <SelectContent>
                                        <SelectItem value="csv">CSV</SelectItem>
                                        <SelectItem value="json">JSON</SelectItem>
                                        <SelectItem value="xml">XML</SelectItem>
                                        <SelectItem value="parquet">Parquet</SelectItem>
                                        <SelectItem value="excel">Excel</SelectItem>
                                    </SelectContent>
                                </Select>
                                <FormMessage />
                            </FormItem>
                        )}
                    />

                    <FormField
                        control={form.control}
                        name="encoding"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Encoding</FormLabel>
                                <Select onValueChange={field.onChange} defaultValue={field.value}>
                                    <FormControl>
                                        <SelectTrigger>
                                            <SelectValue />
                                        </SelectTrigger>
                                    </FormControl>
                                    <SelectContent>
                                        <SelectItem value="utf-8">UTF-8</SelectItem>
                                        <SelectItem value="ascii">ASCII</SelectItem>
                                        <SelectItem value="iso-8859-1">ISO-8859-1</SelectItem>
                                    </SelectContent>
                                </Select>
                                <FormMessage />
                            </FormItem>
                        )}
                    />
                </div>

                {form.watch("format") === "csv" && (
                    <FormField
                        control={form.control}
                        name="delimiter"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Delimiter</FormLabel>
                                <FormControl>
                                    <Input placeholder="," {...field} />
                                </FormControl>
                                <FormDescription>Character used to separate values</FormDescription>
                                <FormMessage />
                            </FormItem>
                        )}
                    />
                )}

                <Button type="submit" className="w-full" disabled={!file}>
                    Create Integration
                </Button>
            </form>
        </Form>
    );
}
