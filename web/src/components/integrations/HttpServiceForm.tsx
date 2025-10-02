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
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { mockApi } from "@/lib/mockApi";
import { toast } from "@/hooks/use-toast";

const httpServiceSchema = z.object({
  name: z.string().min(1, "Name is required"),
  url: z.string().url("Must be a valid URL"),
  method: z.enum(["GET", "POST", "PUT", "DELETE"]),
  headers: z.string().optional(),
  authType: z.enum(["none", "basic", "bearer", "oauth2"]),
  authToken: z.string().optional(),
  requestStructure: z.string().optional(),
  responseStructure: z.string().optional(),
});

type HttpServiceFormValues = z.infer<typeof httpServiceSchema>;

interface HttpServiceFormProps {
  onSuccess: () => void;
}

export function HttpServiceForm({ onSuccess }: HttpServiceFormProps) {
  const form = useForm<HttpServiceFormValues>({
    resolver: zodResolver(httpServiceSchema),
    defaultValues: {
      name: "",
      url: "",
      method: "GET",
      headers: "",
      authType: "none",
      authToken: "",
      requestStructure: "",
      responseStructure: "",
    },
  });

  const onSubmit = async (data: HttpServiceFormValues) => {
    try {
      await mockApi.createIntegration({
        name: data.name,
        type: "service",
        status: "connected",
        description: `${data.method} API endpoint at ${new URL(data.url).hostname}`,
        config: data,
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
        <FormField
          control={form.control}
          name="name"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Integration Name</FormLabel>
              <FormControl>
                <Input placeholder="Salesforce API" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <FormField
          control={form.control}
          name="url"
          render={({ field }) => (
            <FormItem>
              <FormLabel>API URL</FormLabel>
              <FormControl>
                <Input placeholder="https://api.example.com/v1" {...field} />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <div className="grid grid-cols-2 gap-4">
          <FormField
            control={form.control}
            name="method"
            render={({ field }) => (
              <FormItem>
                <FormLabel>HTTP Method</FormLabel>
                <Select onValueChange={field.onChange} defaultValue={field.value}>
                  <FormControl>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                  </FormControl>
                  <SelectContent>
                    <SelectItem value="GET">GET</SelectItem>
                    <SelectItem value="POST">POST</SelectItem>
                    <SelectItem value="PUT">PUT</SelectItem>
                    <SelectItem value="DELETE">DELETE</SelectItem>
                  </SelectContent>
                </Select>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="authType"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Authentication</FormLabel>
                <Select onValueChange={field.onChange} defaultValue={field.value}>
                  <FormControl>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                  </FormControl>
                  <SelectContent>
                    <SelectItem value="none">None</SelectItem>
                    <SelectItem value="basic">Basic Auth</SelectItem>
                    <SelectItem value="bearer">Bearer Token</SelectItem>
                    <SelectItem value="oauth2">OAuth 2.0</SelectItem>
                  </SelectContent>
                </Select>
                <FormMessage />
              </FormItem>
            )}
          />
        </div>

        {form.watch("authType") !== "none" && (
          <FormField
            control={form.control}
            name="authToken"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Auth Token / Credentials</FormLabel>
                <FormControl>
                  <Input type="password" placeholder="••••••••" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
        )}

        <FormField
          control={form.control}
          name="headers"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Headers (JSON)</FormLabel>
              <FormControl>
                <Textarea
                  placeholder='{"Content-Type": "application/json"}'
                  className="font-mono text-sm"
                  {...field}
                />
              </FormControl>
              <FormDescription>Optional custom headers</FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />

        <FormField
          control={form.control}
          name="requestStructure"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Request Structure (JSON)</FormLabel>
              <FormControl>
                <Textarea
                  placeholder='{"field1": "value", "field2": "value"}'
                  className="font-mono text-sm"
                  {...field}
                />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <FormField
          control={form.control}
          name="responseStructure"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Expected Response Structure (JSON)</FormLabel>
              <FormControl>
                <Textarea
                  placeholder='{"data": [], "status": "success"}'
                  className="font-mono text-sm"
                  {...field}
                />
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />

        <Button type="submit" className="w-full">
          Create Integration
        </Button>
      </form>
    </Form>
  );
}
