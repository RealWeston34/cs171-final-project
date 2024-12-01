from llm_service import LLMService
import os

def main():
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("Please set your GEMINI_API_KEY environment variable")
        return
    
    service = LLMService(api_key)
    
    while True:
        print("\nLLM Service Test Menu:")
        print("1. Create new context")
        print("2. Add query to context")
        print("3. View a context")
        print("4. View all contexts")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ")
        
        if choice == "1":
            context_id = input("Enter context ID: ")
            if service.create_context(context_id):
                print(f"Created context: {context_id}")
            else:
                print("Context already exists!")
                
        elif choice == "2":
            context_id = input("Enter context ID: ")
            query = input("Enter your query: ")
            response = service.add_query(context_id, query)
            if response:
                print("\nGemini's response:")
                print(response)
                if service.save_answer(context_id, response):
                    print("Response saved to context")
            else:
                print("Context not found!")
                
        elif choice == "3":
            context_id = input("Enter context ID: ")
            context = service.get_context(context_id)
            if context:
                print("\nContext content:")
                print(context)
            else:
                print("Context not found!")
                
        elif choice == "4":
            contexts = service.get_all_contexts()
            print("\nAll contexts:")
            for cid, content in contexts.items():
                print(f"\n=== Context: {cid} ===")
                print(content)
                
        elif choice == "5":
            print("Goodbye!")
            break
            
        else:
            print("Invalid choice!")

if __name__ == "__main__":
    main()